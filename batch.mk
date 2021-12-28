# Tasks for managing AWS batch jobs and resources
SHELL := /bin/bash

region = us-east-1

ECR_URI = $(AWS_ACCOUNT_ID).dkr.ecr.$(region).amazonaws.com/infer

ifdef YEAR
ifdef MONTH
ifdef QUEUE

submit-job:
	aws batch submit-job --job-name calls-$(YEAR)-$(MONTH) --job-queue $(QUEUE) \
		--job-definition infer \
		--container-overrides "environment=[{name=YEAR,value=$(YEAR)},{name=MONTH,value=$(MONTH)},{name=INFERNOFLAGS,value=--incomplete --quiet --epsg=3628}]" \
		--array-properties size=31

endif
endif
endif

# first, log in with:
# aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com
build:
	docker build -t infer:latest .
	docker tag infer:latest $(ECR_URI):latest
	docker push $(ECR_URI):latest
