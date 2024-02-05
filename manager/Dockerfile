FROM pypy:3

ARG DB_HOST
ARG DB_PORT

ENV DB_HOST=${DB_HOST}
ENV DB_PORT=${DB_PORT}

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python3", "-u", "./main.py", "bible.txt" ]