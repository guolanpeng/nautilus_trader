use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::Context;
use nautilus_persistence::backend::catalog::ParquetDataCatalog;
use tokio::task::JoinHandle;

const CONVERSION_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, PartialEq, Eq)]
struct ClosedFeatherFile {
    data_type: String,
    file_path: PathBuf,
    object_path: String,
}

pub(crate) fn spawn_stream_conversion_task(
    catalog_path: PathBuf,
    instance_id: String,
    data_types: BTreeSet<&'static str>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(CONVERSION_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            let catalog_path = catalog_path.clone();
            let instance_id = instance_id.clone();
            let data_types = data_types.clone();

            match tokio::task::spawn_blocking(move || {
                convert_closed_feather_files(&catalog_path, &instance_id, &data_types)
            })
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(err)) => log::warn!("stream conversion pass failed: {err:?}"),
                Err(err) => log::warn!("stream conversion task join failed: {err}"),
            }
        }
    })
}

fn convert_closed_feather_files(
    catalog_path: &Path,
    instance_id: &str,
    data_types: &BTreeSet<&'static str>,
) -> anyhow::Result<()> {
    let files = collect_closed_feather_files(catalog_path, instance_id, data_types)?;
    if files.is_empty() {
        return Ok(());
    }

    let mut catalog = ParquetDataCatalog::new(catalog_path, None, None, None, None);

    for file in files {
        match catalog.convert_stream_file_to_data(&file.data_type, &file.object_path, false) {
            Ok(()) => {
                if let Err(err) = delete_converted_feather_file(&file) {
                    log::warn!(
                        "failed to delete converted feather file {}: {err:?}",
                        file.file_path.display()
                    );
                }
            }
            Err(err) => {
                log::warn!(
                    "failed to convert feather file {}: {err:?}",
                    file.file_path.display()
                );
            }
        }
    }

    Ok(())
}

fn delete_converted_feather_file(file: &ClosedFeatherFile) -> anyhow::Result<()> {
    std::fs::remove_file(&file.file_path)
        .with_context(|| format!("failed to delete {}", file.file_path.display()))
}

fn collect_closed_feather_files(
    catalog_path: &Path,
    instance_id: &str,
    data_types: &BTreeSet<&'static str>,
) -> anyhow::Result<Vec<ClosedFeatherFile>> {
    let mut files = Vec::new();
    let live_root = catalog_path.join("live").join(instance_id);

    for data_type in data_types {
        let data_dir = live_root.join(data_type);
        if !data_dir.exists() {
            continue;
        }

        for entry in std::fs::read_dir(&data_dir)
            .with_context(|| format!("failed to read data directory {}", data_dir.display()))?
        {
            let entry = entry?;
            let identifier_dir = entry.path();
            if !identifier_dir.is_dir() {
                continue;
            }

            let mut feather_files = std::fs::read_dir(&identifier_dir)
                .with_context(|| {
                    format!(
                        "failed to read identifier directory {}",
                        identifier_dir.display()
                    )
                })?
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .filter(|path| path.extension().is_some_and(|ext| ext == "feather"))
                .collect::<Vec<_>>();

            if feather_files.len() < 2 {
                continue;
            }

            feather_files.sort();
            feather_files.pop();

            for file_path in feather_files {
                let object_path = local_path_to_object_path(catalog_path, &file_path)?;
                files.push(ClosedFeatherFile {
                    data_type: (*data_type).to_string(),
                    file_path,
                    object_path,
                });
            }
        }
    }

    Ok(files)
}

fn local_path_to_object_path(catalog_path: &Path, file_path: &Path) -> anyhow::Result<String> {
    let relative = file_path.strip_prefix(catalog_path).with_context(|| {
        format!(
            "failed to make {} relative to {}",
            file_path.display(),
            catalog_path.display()
        )
    })?;

    Ok(relative.to_string_lossy().replace('\\', "/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_temp_dir() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "nautilus-collector-conversion-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn skips_single_feather_file() {
        let temp_dir = unique_temp_dir();
        let instance_id = "instance";
        let quote_dir = temp_dir
            .join("live")
            .join(instance_id)
            .join("quotes")
            .join("BTCUSD");
        std::fs::create_dir_all(&quote_dir).unwrap();
        std::fs::write(quote_dir.join("BTCUSD_1.feather"), b"").unwrap();

        let files =
            collect_closed_feather_files(&temp_dir, instance_id, &BTreeSet::from(["quotes"]))
                .unwrap();

        assert!(files.is_empty());
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[test]
    fn keeps_latest_feather_file_per_identifier_directory() {
        let temp_dir = unique_temp_dir();
        let instance_id = "instance";
        let quote_dir = temp_dir
            .join("live")
            .join(instance_id)
            .join("quotes")
            .join("BTCUSD");
        std::fs::create_dir_all(&quote_dir).unwrap();
        std::fs::write(quote_dir.join("BTCUSD_1.feather"), b"").unwrap();
        std::fs::write(quote_dir.join("BTCUSD_2.feather"), b"").unwrap();
        std::fs::write(quote_dir.join("BTCUSD_3.feather"), b"").unwrap();

        let files =
            collect_closed_feather_files(&temp_dir, instance_id, &BTreeSet::from(["quotes"]))
                .unwrap();

        let object_paths = files
            .iter()
            .map(|file| file.object_path.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            object_paths,
            vec![
                "live/instance/quotes/BTCUSD/BTCUSD_1.feather",
                "live/instance/quotes/BTCUSD/BTCUSD_2.feather",
            ]
        );

        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[test]
    fn does_not_combine_feather_counts_across_identifier_directories() {
        let temp_dir = unique_temp_dir();
        let instance_id = "instance";
        let base_dir = temp_dir.join("live").join(instance_id).join("quotes");
        std::fs::create_dir_all(base_dir.join("BTCUSD")).unwrap();
        std::fs::create_dir_all(base_dir.join("ETHUSD")).unwrap();
        std::fs::write(base_dir.join("BTCUSD").join("BTCUSD_1.feather"), b"").unwrap();
        std::fs::write(base_dir.join("ETHUSD").join("ETHUSD_1.feather"), b"").unwrap();

        let files =
            collect_closed_feather_files(&temp_dir, instance_id, &BTreeSet::from(["quotes"]))
                .unwrap();

        assert!(files.is_empty());
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[test]
    fn deletes_converted_feather_file() {
        let temp_dir = unique_temp_dir();
        let instance_id = "instance";
        let quote_dir = temp_dir
            .join("live")
            .join(instance_id)
            .join("quotes")
            .join("BTCUSD");
        std::fs::create_dir_all(&quote_dir).unwrap();
        let file_path = quote_dir.join("BTCUSD_1.feather");
        std::fs::write(&file_path, b"").unwrap();

        let file = ClosedFeatherFile {
            data_type: "quotes".to_string(),
            file_path: file_path.clone(),
            object_path: "live/instance/quotes/BTCUSD/BTCUSD_1.feather".to_string(),
        };

        delete_converted_feather_file(&file).unwrap();

        assert!(!file_path.exists());

        std::fs::remove_dir_all(temp_dir).unwrap();
    }
}
