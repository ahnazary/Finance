FROM python:3.11

# Set the working directory in the container
WORKDIR /app

COPY finance/requirements.txt .
COPY finance/schedule_jobs schedule_jobs
COPY config.py .
COPY finance/src src

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# run all files in schedule_jobs folder
CMD python -m schedule_jobs.annual_balance_sheet && \
    python -m schedule_jobs.annual_cashflow && \
    python -m schedule_jobs.annual_income_sttm && \
    python -m schedule_jobs.annual_balance_sheet && \
    python -m schedule_jobs.quarterly_cashflow && \
    python -m schedule_jobs.quarterly_income_sttm && \
    python -m schedule_jobs.quarterly_balance_sheet && \
    python -m schedule_jobs.quarterly_cashflow