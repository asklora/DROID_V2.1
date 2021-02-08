# dlpm_master2

[![N|Solid](https://static.wixstatic.com/media/075d6e_47fc97181d9b45c5a137555804c5cfb2~mv2.jpg/v1/fill/w_464,h_331,al_c,q_80,usm_0.66_1.00_0.01/075d6e_47fc97181d9b45c5a137555804c5cfb2~mv2.webp)](https://nodesource.com/products/nsolid)

![Build Status](https://travis-ci.org/joemccann/dillinger.svg?branch=master)

Driver Requirements:

  - tensorflow-gpu v 2.0.0
  - CUDDN v 7.6.2
  - CUDA v 10.0

# Check Cuda and Cuddn version:

  - Cudda version check  
  - ```cat /usr/local/cuda/version.txt```
  - Cuddn version check 
  - ```cat /usr/include/cudnn.h | grep CUDNN_MAJOR -A 2```
# Driver instalations:

  - Cuddn Installations
  - [![N](https://developer.nvidia.com/sites/all/themes/devzone_base/images/nvidia.png)](https://docs.nvidia.com/deeplearning/sdk/cudnn-install/index.html)
  - CUDA Installations
  - [![N](https://developer.nvidia.com/sites/all/themes/devzone_base/images/nvidia.png)](https://developer.nvidia.com/cuda-downloads?target_os=Linux&target_arch=x86_64&target_distro=Ubuntu&target_version=1804&target_type=debnetwork)


### Setup
make environment variable python3.6. and prerequisites for psycopg2.

```sh
$ sudo apt-get install libpq-dev 
$ sudo apt-get install python3.6-dev
$ sudo apt install virtualenv
$ virtualenv env -p python3.6
$ source env/bin/activate
```
download repo...

```sh
$ git clone https://github.com/stepchoi/dlpm_master2.git
$ cd dlpm_master2
$ pip install -r requirements.txt
```

