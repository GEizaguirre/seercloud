# Seer documentation

## Requirements

Seer has been developed in Ubuntu 20.04.4 and tested with Python 3.8, although
it should be potentially executable with any Python version over 3.6. As it
is more of a research prototype than a fully fledged framework, we recommend
testing it on its native development technologies.

## <a name="install"></a>Installation

Simply download the source code from the GitHub repository and install the library through pip.

```bash
wget https://github.com/GEizaguirre/seercloud/archive/refs/heads/main.zip
unzip main.zip
cd seercloud-main
pip3 install .
```

## Configuration

Seer relies on Lithops to configure its backend components. Configuration of cloud functions and object storage is straightforward, and it can be performed using free tier accounts of most clouds.
For IBM Cloud, for instance, fill a yaml configuration file following the Lithops guideline.

* [Configuration of IBM Cloud Functions](https://github.com/lithops-cloud/lithops/blob/master/docs/source/compute_config/ibm_cf.md)
* [Configuration of IBM Cloud Object Storage](https://github.com/lithops-cloud/lithops/blob/master/docs/source/storage_config/ibm_cos.md)

For Seer's native cloud functions & object storage architecture, the configuration file should be structured as following.

```yaml
lithops:
    backend    :    ibm_cf
    storage    :    ibm_cos

ibm_cf:
    endpoint    :    https://xx-xx.functions.cloud.ibm.com
    namespace    :    xxxxxxxxx
    api_key    :    xxxxxxxxxxx
    runtime_memory : mmmm

ibm_cos:
    storage_bucket    :    bucket_name
    endpoint    :    https://s3.xx-xx.cloud-object-storage.appdomain.cloud
    private_endpoint    :    https://s3.private.xx-xx.cloud-object-storage.appdomain.cloud
    access_key    :    xxxxxxxxxxxxx
    secret_key    :    xxxxxxxxxxxxx
```


We recommend loading your configuration yaml file explicitly using the yaml python library. For instance:
```python
import yaml
config = yaml.safe_load(open("../config.yaml", "r"))
```

## Execution example

Jobs are Seer's pipeline execution managers. Stages and operations must be added to the job
programmatically. Configuration parameters must be transferred to Seer as an argument of each Job.
The code in the example loads the configuration automatically from ```../config/config_cloud.yaml```.

[Example code for a parallel sort](../tests/pipelines/cloud/sort.py)

```python
from seercloud.scheduler import Job
from seercloud.operation import Scan, Exchange, Sort, Write
from tests.config import cloud_config

job = Job ( num_stages = 2, lithops_config = cloud_config())
job.add(stage = 0, op = Scan, file ="terasort_1GB.csv", bucket ="seer-data2")
job.add( stage = 0, op = Exchange )
job.add( stage = 1, op = Sort, key = "0" )
job.add( stage = 1, op = Write, bucket = "seer-data2", key = "terasort_1GB_sorted")
job.dependency ( parent = 0, child = 1)
job.run()
```

For a straightforward, end-to-end run of Seer, from installation, configuration to
a basic, parallel sort execution, steps are exactly the following.

1. Start an empty VM with Ubuntu 20.04, python 3.8 and pip installed.

```bash
sudo apt update
sudo apt install python3.8
sudo apt install python3-pip
sudo apt install unzip
```

2. Copy the configuration file into a directory named ```config```. 
The file should have the name ```config_cloud.yaml```. The ```config``` directory
should be placed on seercloud execution path's parent directory.

```bash
mkdir config
cp config.yaml config/config_cloud.yaml
```

3. Execute the [installation](#a-nameinstallainstallation) code described above.

```bash
wget https://github.com/GEizaguirre/seercloud/archive/refs/heads/main.zip
unzip seercloud-dev-main.zip
mv seercloud-dev-main seercloud
cd seercloud
pip3 install .
```

4. Run the following command.

```bash
python3.8 tests/pipeline/sort.py
```

