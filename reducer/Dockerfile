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

# Définir la commande pour exécuter le script
CMD ["python3", "-u", "./reducer.py"]
