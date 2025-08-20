/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
include { CELLPROFILER_LOAD_DATA_CSV as ILLUMINATION_LOAD_DATA_CSV } from '../cellprofiler_load_data_csv'
include { CELLPROFILER_ILLUMINATIONCORRECTION } from '../../../modules/local/cellprofiler/illuminationcorrection'

workflow CELLPAINTING {

    take:
    ch_samplesheet_cp
    cppipes

    main:

    ILLUMINATION_LOAD_DATA_CSV (
        ch_samplesheet_cp,
        ['batch', 'plate'],
        'illumination'
    )

    ILLUMINATION_LOAD_DATA_CSV.out.images_with_load_data_csv
        .flatMap { group_meta, meta_list, image_list, csv_file ->

            // Create a tuple for each metadata entry with the full image list and the CSV file
            return meta_list.collect { meta ->
                [group_meta, meta, image_list, csv_file]
            }
        }
        .set { ch_images_with_csv }

    CELLPROFILER_ILLUMINATIONCORRECTION (
        ch_images_with_csv,
        cppipes[0]
    )






    // emit:
    // // TODO nf-core: edit emitted channels
    // versions = ch_versions                     // channel: [ versions.yml ]
}


/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

/**
   * Generate CellProfiler load_data.csv content from channel data
   * 
   * @param channel_data List of [meta, image] tuples
   * @param config Configuration map with the following keys:
   *   - grouping_keys: List of metadata keys to group by (e.g., ['batch','plate','channel'])
   *   - row_keys: List of metadata keys that define unique rows (e.g., ['well','site','batch','plate','col','row'])
   *   - channel_key: Metadata key that identifies image channels (default: 'channel')
   *   - metadata_columns: List of metadata keys to include as columns (default: ['batch','plate','well','col','row','site'])
   *   - image_channel_prefix: Prefix for image filename columns (default: 'FileName_Orig')
   *   - metadata_column_prefix: Prefix for metadata columns (default: 'Metadata_')
   * @return Map with group_id as key and csv_content as value
   */
def generateCellProfilerLoadDataCsv(List channel_data, Map config) {
    // Set defaults
    def grouping_keys = config.grouping_keys ?: ['batch', 'plate', 'channel']
    def row_keys = config.row_keys ?: ['well', 'site', 'batch', 'plate', 'col', 'row']
    def channel_key = config.channel_key ?: 'channel'
    def metadata_columns = config.metadata_columns ?: ['batch', 'plate', 'well', 'col', 'row', 'site']
    def image_channel_prefix = config.image_channel_prefix ?: 'FileName_Orig'
    def metadata_column_prefix = config.metadata_column_prefix ?: 'Metadata_'

    // Group data by specified grouping keys
    def grouped_data = channel_data.groupBy { meta, image ->
        grouping_keys.collect { meta[it] }.join('_')
    }

    // Process each group
    return grouped_data.collectEntries { group_id, group_items ->

        // Extract unique channels from this group
        def image_channels = group_items.collect { meta, image ->
            meta[channel_key]
        }.unique().sort()

        // Create header
        def image_headers = image_channels.collect { "${image_channel_prefix}${it}" }
        def metadata_headers = metadata_columns.collect { "${metadata_column_prefix}${it.capitalize()}" }
        def header = (image_headers + metadata_headers).join(',')

        // Group by row keys to create unique rows
        def rows_data = group_items.groupBy { meta, image ->
            row_keys.collectEntries { key -> [key, meta[key]] }
        }

        // Generate CSV rows
        def csv_rows = rows_data.collect { row_meta, row_items ->

            // Create channel-to-image mapping for this row
            def images_by_channel = row_items.collectEntries { meta, image ->
                [meta[channel_key], image.name]
            }

            // Build image filename columns in channel order
            def image_filenames = image_channels.collect { channel ->
                images_by_channel[channel] ?: ""
            }

            // Build metadata columns
            def metadata_values = metadata_columns.collect { col ->
                row_meta[col] ?: ""
            }

            // Combine and create CSV row
            (image_filenames + metadata_values).join(',')
        }

        // Combine header and rows
        def csv_content = ([header] + csv_rows).join('\n')

        [group_id, csv_content]
    }
}

/**
* Channel operator extension to apply the CellProfiler CSV generation
* Usage: channel.generateCellProfilerCsv(config)
*/
def generateCellProfilerCsv(config = [:]) {
    return this.collect().map { channel_data ->
        generateCellProfilerLoadDataCsv(channel_data, config)
    }
}