FROM python:3.8

# Install google chrome for selenium scraping
RUN apt-get update && apt-get install -y \
	apt-transport-https \
	ca-certificates \
	curl \
	gnupg \
	--no-install-recommends \
	&& curl -sSL https://dl.google.com/linux/linux_signing_key.pub | apt-key add - \
	&& echo "deb https://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
	&& apt-get update && apt-get install -y \
	google-chrome-stable \
	fontconfig \
	fonts-ipafont-gothic \
	fonts-wqy-zenhei \
	fonts-thai-tlwg \
	fonts-kacst \
	fonts-symbola \
	fonts-noto \
	fonts-freefont-ttf \
	--no-install-recommends \
	&& apt-get purge --auto-remove -y curl gnupg \
	&& rm -rf /var/lib/apt/lists/*

# Install airflow
RUN pip install apache-airflow==2.2.2 apache-airflow-providers-postgres apache-airflow-providers-amazon

# Install dbt
RUN pip install dbt-core==1.0.3  dbt-postgres==1.0.3

# Install requirements last to avoid reinstalling everything
# when requirements change
COPY ./airflow/requirements.txt airflow/requirements.txt
RUN pip install -r /airflow/requirements.txt

ENV AIRFLOW_HOME=/airflow

EXPOSE 8080
ENTRYPOINT ["bash", "/airflow/entrypoint.sh"]


