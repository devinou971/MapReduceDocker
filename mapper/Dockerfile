FROM pypy:3

ARG DB_HOST
ARG DB_PORT

ENV DB_HOST=${DB_HOST}
ENV DB_PORT=${DB_PORT}

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copier le script du mappeur dans le conteneur
COPY . .

CMD ["python3", "-u", "./mapper.py"]
