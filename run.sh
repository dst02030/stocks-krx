#!/bin/bash
set -e
cur_dir=$(cd $(dirname $0) && pwd)

if [ -n $CONDA_VOLUME ]; then
    export CONDA_VOLUME=/home/heenj/anaconda3
fi

CONDA_ENV=jupyter
source $CONDA_VOLUME/etc/profile.d/conda.sh


# if conda env list | grep -q $CONDA_ENV; then
#     echo "Conda environment does not exist.."
#     echo "Creates Conda env..."
# fi

conda activate $CONDA_ENV


set -a
source $cur_dir/conf/credentials
set +a

if [ -z $1 ]; then
    echo "You should enter suburl info."
    exit 1
fi


echo "Current time is `date`."
export _ts=`date +"%Y-%m-%d %H:%M:%S %z"`



echo "Run krx module!"
python3 $cur_dir/main.py $1

echo "Finish time is `date`."
conda deactivate


