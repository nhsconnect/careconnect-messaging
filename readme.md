In this directory

mvn install 

docker build . -t ccri-messaging

docker tag ccri-messaging thorlogic/ccri-messaging

docker push thorlogic/ccri-messaging

docker run -d -p 8182:8182 ccri-messaging 

