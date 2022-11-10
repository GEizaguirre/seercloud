<p align="center">
  <a href="https://github.com/GEizaguirre/seercloud">
    <h1 id='lithops' align="center"><img src="fig/seer-logo-big.png" alt="Seer" width = 150/></h1>
  </a>
</p>

[![DOI](https://zenodo.org/badge/545037642.svg)](https://zenodo.org/badge/latestdoi/545037642)

Seer is serverless data analytics framework with dynamic optimization of data exchange steps. It is built on [Lithops](https://github.com/lithops-cloud/lithops), a multi-cloud distributed computing framework, over cloud functions and blob object storage.

Documentation and execution instructions are available at [Documentation](documentation/README.md).



## Programatical API

```python
import yaml

from seercloud.scheduler import Job
from seercloud.operation import Scan, Exchange, Sort

job = Job ( num_stages = 2, lithops_config = yaml.load(open("config.yaml", "rb")))
job.add(stage = 0, op = Scan, file ="terasort_1GB.csv", bucket ="seer-data")
job.add( stage = 0, op = Exchange )
job.add( stage = 1, op = Sort, key = "0" )
job.dependency ( parent = 0, child = 1)
job.run()
```


# Acknowledgements

![image](https://user-images.githubusercontent.com/26366936/61350554-d62acf00-a85f-11e9-84b2-36312a35398e.png)

This project has received funding from the European Union's Horizon 2020 research and innovation programme under grant agreement No 825184.

