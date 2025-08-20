
// This subworkflow takes images from a samplesheet and creates
// CellProfiler-compatible load_data.csv files grouped by specified metadata keys

workflow CELLPROFILER_LOAD_DATA_CSV {

    take:
    ch_samplesheet // channel: [ val(meta), [ image ] ]
    grouping_keys  // value channel: list of keys to group by (e.g., ['batch','plate','channel'])
    step_name      // value channel: name of the pipeline step (determines working directory for load_data.csv files)

    main:

    // Group images by the specified metadata keys
    ch_samplesheet
        .map { meta, image ->
            def keys = grouping_keys
            def group_key = meta.subMap(keys)
            def group_id = keys.collect { meta[it] }.join('_')

            [group_key + [id: group_id], meta, image]
        }
        .groupTuple()
        .set { ch_images_grouped }

    // Create load_data.csv files for each group
    ch_images_grouped
        .map { group_meta, meta_list, image_list ->
            // Derive channels dynamically from the metadata in this group
            def image_channels = meta_list.collect { it.channels }.unique()

            // def header = "FileName_Orig" + image_channels.join(',FileName_Orig') + ",Metadata_Batch,Metadata_Plate,Metadata_Well,Metadata_Col,Metadata_Row,Metadata_Site"
            def header = "Metadata_Plate,Metadata_Well,Metadata_Site," + "FileName_Orig" + image_channels.join(',FileName_Orig')+ "," + "Frame_Orig" + image_channels.join(',Frame_Orig')

            // Group by well+site within this group so we can create a single row per well+site
            def grouped_by_well = [meta_list, image_list].transpose().collect { meta, image ->
                def group_key = meta.subMap(['plate','well','batch','site'])
                [group_key, meta.channels, image]
            }.groupBy { it[0] }

            def rows = grouped_by_well.collect { row_meta, entries ->

                // Extract channels and images from the entries
                def row_channels = entries.collect { it[1] }  // Extract channels
                def row_images = entries.collect { it[2] }    // Extract images

                // Create a dictionary with the image_channels as keys and the images as values
                def images_by_channel = [row_channels, row_images].transpose().collectEntries { channels, image ->
                    [channels, image]
                }

                // Create a list of the image filenames in the order of the image_channels
                def image_filenames = image_channels.collect { channels ->
                    images_by_channel[channels] ? "\"${images_by_channel[channels].name}\"" : "\"\""
                }

                // Create a list of the image frames in the order of the image_channels
                def image_frames = image_channels.collect { channels ->
                    images_by_channel[channels] ? image_channels.indexOf(channels) : ""
                }

                // Add metadata columns
                def row = [row_meta.plate, row_meta.well, row_meta.site] + image_filenames + image_frames
                row.join(',')
            }

            def csv_content = ([header] + rows).join('\n')
            [group_meta, csv_content]
        }
        .collectFile(
            newLine: true,
            storeDir: "${workflow.workDir}/${workflow.sessionId}/cellprofiler/load_data_csvs/${step_name}"
        ) { group_meta, csv_content ->
            ["${group_meta.id}.csv", csv_content]
        }
        .set { ch_load_data_csvs }

    // Join grouped images with their corresponding CSV files
    ch_images_grouped
        .map { group_meta, meta_list, image_list ->
            [group_meta.id, group_meta, meta_list, image_list]
        }
        .set { ch_images_with_key }

    ch_load_data_csvs
        .map { load_data_csv ->
            [load_data_csv.baseName, load_data_csv]
        }
        .set { ch_csvs_with_key }

    // Combine grouped images with their load_data.csv files
    ch_images_with_key
        .join(ch_csvs_with_key)
        .map { _key, group_meta, meta_list, image_list, load_data_csv ->
            // Remove duplicate image paths while preserving order
            def unique_image_list = image_list.unique() // Since we can point to the same image multiple times, we need to remove duplicates
            [group_meta, meta_list.channels.unique(), unique_image_list, load_data_csv]
        }
        .set { ch_images_with_load_data_csv }

    emit:
    images_with_load_data_csv      = ch_images_with_load_data_csv    // channel: [ val(meta), [ list_of_images ], load_data_csv ]

}
