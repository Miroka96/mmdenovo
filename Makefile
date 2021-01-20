
JUPYTER_IMAGE = tensorflow/tensorflow:latest-gpu-jupyter
JUPYTER_LOCAL_PORT = 8888
JUPYTER_PORT_PUBLISHING = -p $(JUPYTER_LOCAL_PORT):8888
JUPYTER_VOLUME_MOUNTING = -v ${PWD}:/tf/workspace
JUPYTER_CONTAINER_NAME = gpu_jupyter
JUPYTER_GPU = --runtime=nvidia
JUPYTER_PARAMETERS = $(JUPYTER_GPU) $(JUPYTER_VOLUME_MOUNTING) $(JUPYTER_PORT_PUBLISHING) $(JUPYTER_IMAGE)

THERMORAWFILEPARSER_IMAGE = quay.io/biocontainers/thermorawfileparser:1.2.3--1
THERMORAWFILEPARSER_VOLUME_MOUNTING = -v ${PWD}:/data
THERMORAWFILEPARSER_PARAMETERS = --rm -w /data $(THERMORAWFILEPARSER_VOLUME_MOUNTING) $(THERMORAWFILEPARSER_IMAGE) /bin/bash

.PHONY: monitor-gpu jupyter-pull jupyter-test jupyter-deploy-only jupyter-token jupyter-deploy-prepare jupyter-deploy jupyter-stop jupyter-rm jupyter-undeploy jupyter-redeploy

monitor-gpu:
	watch -n 1 nvidia-smi

jupyter-pull:
	docker pull $(JUPYTER_IMAGE)


jupyter-test: jupyter-pull
	docker run -it --rm $(JUPYTER_PARAMETERS)

jupyter-deploy-only: jupyter-pull
	docker run --restart always --detach --name=$(JUPYTER_CONTAINER_NAME) $(JUPYTER_PARAMETERS)
	sleep 2

jupyter-deploy-prepare:
	docker exec $(JUPYTER_CONTAINER_NAME) pip install pyteomics pandas pyarrow lxml wget

jupyter-token:
	-docker logs $(JUPYTER_CONTAINER_NAME) 2>&1 | grep \?token\= | tail -n 1 | grep -o 'http.*$$'

jupyter-deploy: | jupyter-deploy-only jupyter-token jupyter-deploy-prepare

jupyter-stop:
	-docker stop $(JUPYTER_CONTAINER_NAME)

jupyter-rm:
	-docker rm $(JUPYTER_CONTAINER_NAME)

jupyter-undeploy: | jupyter-stop jupyter-rm

jupyter-redeploy: | jupyter-undeploy jupyter-deploy

thermorawfileparser-pull:
	docker pull $(THERMORAWFILEPARSER_IMAGE)

thermorawfileparser-shell: thermorawfileparser-pull
	@echo
	@echo "######################################################"
	@echo type \'ThermoRawFileParser --help\' to get an overview
	@echo "######################################################"
	@echo
	docker run -it $(THERMORAWFILEPARSER_PARAMETERS)

thermorawfileparser-noninteractive: thermorawfileparser-pull
	# here you can pipe in commands
	docker run -i $(THERMORAWFILEPARSER_PARAMETERS)
