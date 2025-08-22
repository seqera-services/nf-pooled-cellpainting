process CELLPROFILER_ILLUMINATIONCORRECTION {
    tag "${meta.id}"
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
    
    # Check if illumination_cppipe ends with .template
    if [[ "${illumination_cppipe}" == *.template ]]; then
        # Handle single vs multiple channels for template files
        IFS=',' read -ra CHANNELS <<< "${channels}"
        if [ \${#CHANNELS[@]} -eq 1 ]; then
            # Single channel - simple replacement
            sed 's/{channel}/'\${CHANNELS[0]}'/g; s/{module_num}/2/g; s/{final_module_num}/6/g' ${illumination_cppipe} > illumination.cppipe
            total_modules=6  # LoadData + 4 channel modules + CreateBatchFiles
        else
            # Multiple channels - extract template blocks and repeat
            # Extract header (before CHANNEL_BLOCK_START)
            sed -n '1,/# CHANNEL_BLOCK_START/p' ${illumination_cppipe} | sed '\$d' > illumination.cppipe
            
            # Extract channel block template
            sed -n '/# CHANNEL_BLOCK_START/,/# CHANNEL_BLOCK_END/p' ${illumination_cppipe} | sed '1d;\$d' > channel_block.tmp
            
            # Generate blocks for each channel
            module_num=2
            for channel in \${CHANNELS[@]}; do
                sed 's/{channel}/'\$channel'/g; s/{module_num}/'\$module_num'/g' channel_block.tmp >> illumination.cppipe
                echo "" >> illumination.cppipe
                module_num=\$((module_num + 4))
            done
            
            # Add footer (after CHANNEL_BLOCK_END)
            total_modules=\$((1 + \${#CHANNELS[@]} * 4 + 1))
            sed -n '/# CHANNEL_BLOCK_END/,\$p' ${illumination_cppipe} | sed '1d; s/{final_module_num}/'\$module_num'/g' >> illumination.cppipe
            rm channel_block.tmp
        fi
        
        # Update ModuleCount in header and remove any remaining markers
        sed -i 's/ModuleCount:5/ModuleCount:'\$total_modules'/; /^# CHANNEL_BLOCK_/d' illumination.cppipe
    else
        # Not a template file - use as-is
        cp ${illumination_cppipe} illumination.cppipe
    fi

    cellprofiler -c -r \
    ${task.ext.args ?: ''} \
    -p illumination.cppipe \
    -o illumination_corrections \
    --data-file=${load_data_csv} \
    --image-directory ./images/ \
    -g Metadata_Plate=${meta.plate}

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
