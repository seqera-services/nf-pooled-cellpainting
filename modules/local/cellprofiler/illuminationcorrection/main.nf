process CELLPROFILER_ILLUMINATIONCORRECTION {
    tag "${meta.id}_${channels}"
    label 'process_medium'

    conda "${moduleDir}/environment.yml"
    container "${workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container
        ? 'https://depot.galaxyproject.org/singularity/cellprofiler:4.2.8--pyhdfd78af_0'
        : 'community.wave.seqera.io/library/cellprofiler:4.2.8--aff0a99749304a7f'}"

    input:
    tuple val(meta), val(channels), path(images, stageAs: "images/*"), path(load_data_csv)

    path illumination_cppipe

    output:
    tuple val(meta), path("illumination_corrections/*.npy"), emit: illumination_corrections
    path "versions.yml", emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    """
    mkdir -p illumination_corrections
    # Replace the channel name in the cppipe file
    sed 's/{{channel}}/${channels}/g' ${illumination_cppipe} > illumination.cppipe

    cellprofiler -c -r \
    ${args} \
    -p illumination.cppipe \
    -o illumination_corrections \
    --data-file=${load_data_csv} \
    --image-directory ./images/ \
    -g Metadata_Plate=${meta.plate} \

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        cellprofiler: \$(cellprofiler --version |& sed '1!d ; s/cellprofiler //')
    END_VERSIONS
    """

    stub:
    """
    mkdir -p illumination_corrections
    echo 'this is not an illumination correction' > illumination_corrections/${meta.plate}_Illum${meta.channel}.npy

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        cellprofiler: \$(cellprofiler --version |& sed '1!d ; s/cellprofiler //')
    END_VERSIONS
    """
}
