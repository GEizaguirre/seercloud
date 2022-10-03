<p align="center">
  <a href="https://github.com/GEizaguirre/seercloud">
    <h1 id='lithops' align="center"><img src="fig/seer-logo-big.png" alt="Seer" width = 150/></h1>
  </a>
</p>


Seer is serverless data analytics framework with dynamic optimization of data exchange steps. It is built on [Lithops](https://github.com/lithops-cloud/lithops), a multi-cloud distributed computing framework, over cloud functions and blob object storage.


## Installation

Simply download the source code from the github repository and install the library through pip:

```bash
cd seercloud
pip install .
```


## Configuration

Seer relies on Lithops to configure its backend components. Configuration of cloud functions and object storage is straightforward, and it can be performed using free tier accounts of most clouds.
For IBM Cloud, for instance, fill a yaml configuration file following the Lithops guideline.

* [Configuration of IBM Cloud Functions](https://github.com/lithops-cloud/lithops/blob/master/docs/source/compute_config/ibm_cf.md)
* [Configuration of IBM Cloud Object Storage](https://github.com/lithops-cloud/lithops/blob/master/docs/source/storage_config/ibm_cos.md) 


## Programatical API

We provide some execution examples in the [tests](https://github.com/GEizaguirre/seercloud/tree/main/tests) directory. 
For instance, one could perform a sort operation on a dataset
as following. 

```python
import yaml

from seercloud.scheduler import Job
from seercloud.operation import Scan, Exchange, Sort

job = Job ( num_stages = 2, lithops_config = yaml.load(open("config.yaml", "rb")))
job.add(stage = 0, op = Scan, file ="terasort_1GB.csv", bucket ="seer-data")
job.add( stage = 0, op = Exchange )
job.add( stage = 1, op = Sort, key = "c0" )
job.dependency ( parent = 0, child = 1)
job.run()
```


# Acknowledgements

![image](https://user-images.githubusercontent.com/26366936/61350554-d62acf00-a85f-11e9-84b2-36312a35398e.png)

This project has received funding from the European Union's Horizon 2020 research and innovation programme under grant agreement No 825184.

