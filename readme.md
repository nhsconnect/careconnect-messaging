In this directory

mvn install 

docker build . -t ccri-tie

docker tag ccri-tie thorlogic/ccri-tie

docker push thorlogic/ccri-tie


docker run -d -p 8182:8182 ccri-tie 

