FROM public.ecr.aws/lambda/python:3.10

COPY *.py ${LAMBDA_TASK_ROOT}
COPY requirements.txt ${LAMBDA_TASK_ROOT}

RUN pip3 install -r requirements.txt -t ${LAMBDA_TASK_ROOT}

CMD [ "rds_snapshot_account_share.lambda_handler" ]