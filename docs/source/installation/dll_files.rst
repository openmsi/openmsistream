Issues with DLL Files
----------------------

On Windows, you need to set a special variable in the virtual environment to allow the Kafka Python code to find its dependencies (see `here <https://github.com/ContinuumIO/anaconda-issues/issues/12475>`_ for more details). To do this, activate your Conda environment as above then type the following commands to set the variable and then refresh the environment::

    conda env config vars set CONDA_DLL_SEARCH_MODIFICATION_ENABLE=1
    conda deactivate 
    conda activate openmsi
