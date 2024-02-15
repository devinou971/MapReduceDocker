docker build .\reducer\ -t info932/reducer:%1
docker build .\mapper\ -t info932/mapper:%1
docker build .\manager\ -t info932/manager:%1

docker build .\reducer\ -t info932/reducer:latest
docker build .\mapper\ -t info932/mapper:latest
docker build .\manager\ -t info932/manager:latest