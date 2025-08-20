/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
include { MULTIQC                } from '../modules/nf-core/multiqc/main'
include { CELLPAINTING           } from '../subworkflows/local/cellpainting'
// include { SEQ_BY_SYNTH           } from '../subworkflows/local/seq_by_synthesis'


include { paramsSummaryMap       } from 'plugin/nf-schema'
include { paramsSummaryMultiqc   } from '../subworkflows/nf-core/utils_nfcore_pipeline'
include { softwareVersionsToYAML } from '../subworkflows/nf-core/utils_nfcore_pipeline'
include { methodsDescriptionText } from '../subworkflows/local/utils_nfcore_nf-pooled-cellpainting_pipeline'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN MAIN WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow POOLED_CELLPAINTING {

    take:
    ch_samplesheet // channel: samplesheet read in from --input
    barcodes       // file: path to barcodes.csv file
    cppipes       // array: paths to cpipe jinja files

    main:

    // ch_versions = Channel.empty()
    // ch_multiqc_files = Channel.empty()

    // Generate barcodes channel from barcodes.csv file
    // ch_barcodes = Channel.fromPath(barcodes, checkIfExists: true)

    // Split ch_samplesheet by assay (CP or SBS)
    ch_samplesheet
        .branch { meta, _images ->
            cp: meta.assay == 'CP'
            sbs: meta.assay == 'SBS'
        }
        .set { ch_samplesheet_split }

    // Split channels by comma to create separate channels for each channel name
    ch_samplesheet_cp = ch_samplesheet_split.cp
        .flatMap { meta, image ->
            // Split channels by comma and create a separate entry for each channel
            def channels_string = meta.channels.split(',')
            if (channels_string.size() > 1) {
                return channels_string.collect { channel_name ->
                    def new_meta = meta.clone()
                    new_meta.remove('channels') // Remove the original comma-separated field
                    new_meta.channels = channel_name.trim()
                    [new_meta, image]
                }
            } else {
                return [[meta, image]]
            }
        }
    
    //ch_samplesheet_sbs = ch_samplesheet_split.sbs

    // Process cell painting (CP) data
    CELLPAINTING (
        ch_samplesheet_cp,
        cppipes
    )

    // Process sequencing by synthesis (SBS) data
    // SEQ_BY_SYNTH(ch_samplesheet_sbs)


    //
    // Collate and save software versions
    //
    // softwareVersionsToYAML(ch_versions)
    //     .collectFile(
    //         storeDir: "${params.outdir}/pipeline_info",
    //         name:  'nf-pooled-cellpainting_software_'  + 'mqc_'  + 'versions.yml',
    //         sort: true,
    //         newLine: true
    //     ).set { ch_collated_versions }


    //
    // MODULE: MultiQC
    //
    // ch_multiqc_config        = Channel.fromPath(
    //     "$projectDir/assets/multiqc_config.yml", checkIfExists: true)
    // ch_multiqc_custom_config = params.multiqc_config ?
    //     Channel.fromPath(params.multiqc_config, checkIfExists: true) :
    //     Channel.empty()
    // ch_multiqc_logo          = params.multiqc_logo ?
    //     Channel.fromPath(params.multiqc_logo, checkIfExists: true) :
    //     Channel.empty()

    // summary_params      = paramsSummaryMap(
    //     workflow, parameters_schema: "nextflow_schema.json")
    // ch_workflow_summary = Channel.value(paramsSummaryMultiqc(summary_params))
    // ch_multiqc_files = ch_multiqc_files.mix(
    //     ch_workflow_summary.collectFile(name: 'workflow_summary_mqc.yaml'))
    // ch_multiqc_custom_methods_description = params.multiqc_methods_description ?
    //     file(params.multiqc_methods_description, checkIfExists: true) :
    //     file("$projectDir/assets/methods_description_template.yml", checkIfExists: true)
    // ch_methods_description                = Channel.value(
    //     methodsDescriptionText(ch_multiqc_custom_methods_description))

    // ch_multiqc_files = ch_multiqc_files.mix(ch_collated_versions)
    // ch_multiqc_files = ch_multiqc_files.mix(
    //     ch_methods_description.collectFile(
    //         name: 'methods_description_mqc.yaml',
    //         sort: true
    //     )
    // )

    // MULTIQC (
    //     ch_multiqc_files.collect(),
    //     ch_multiqc_config.toList(),
    //     ch_multiqc_custom_config.toList(),
    //     ch_multiqc_logo.toList(),
    //     [],
    //     []
    // )

    // emit:multiqc_report = MULTIQC.out.report.toList() // channel: /path/to/multiqc_report.html
    // versions       = ch_versions                 // channel: [ path(versions.yml) ]

}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    THE END
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
