
DOCKER_IMAGE = tensorflow/tensorflow:latest-gpu-jupyter
DOCKER_LOCAL_JUPYTER_PORT = 127.0.0.1:8888
DOCKER_PORT_PUBLISHING = -p $(DOCKER_LOCAL_JUPYTER_PORT):8888
DOCKER_VOLUME_MOUNTING = -v ${PWD}:/tf/workspace

DOCKER_CONTAINER_NAME = gpu_jupyter
DOCKER_GPU = --runtime=nvidia

DOCKER_PARAMETERS = $(DOCKER_GPU) $(DOCKER_VOLUME_MOUNTING) $(DOCKER_PORT_PUBLISHING) $(DOCKER_IMAGE)

.PHONY: monitor-gpu jupyter-pull jupyter-test jupyter-deploy-only jupyter-token jupyter-deploy jupyter-stop jupyter-rm jupyter-undeploy jupyter-redeploy

monitor-gpu:
	watch -n 1 nvidia-smi

jupyter-pull:
	docker pull $(DOCKER_IMAGE)

jupyter-test: jupyter-pull
	docker run -it --rm $(DOCKER_PARAMETERS)

jupyter-deploy-only: jupyter-pull
	docker run --restart always --detach --name=$(DOCKER_CONTAINER_NAME) $(DOCKER_PARAMETERS)
	sleep 1

jupyter-token:
	docker logs $(DOCKER_CONTAINER_NAME) 2>&1 | grep \?token\= | tail -n 1 | grep -o 'http.*$$'

jupyter-deploy: | jupyter-deploy-only jupyter-token

jupyter-stop:
	-docker stop $(DOCKER_CONTAINER_NAME)

jupyter-rm:
	-docker rm $(DOCKER_CONTAINER_NAME)

jupyter-undeploy: | jupyter-stop jupyter-rm

jupyter-redeploy: | jupyter-undeploy jupyter-deploy
