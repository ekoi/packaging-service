# packaging-service
 curl -X 'POST'   'https://transformer.labs.dans.knaw.nl/upload-xsl/form-metadata-to-dataset.xsl/true' -d @/$BASE_DIR/packaging-service/resources/xsl/form-metadata-to-dataset.xsl -H "Authorization: Bearer API-KEY" -H 'Content-Type: application/xml'

curl -X POST  -H "Authorization: Bearer API_KEY" https://utility.packaging.dataverse.tk/inbox/file -F metadataId=metadata-form-name-unique-1234-12341 -F file=@/$BASE_DIR/packaging-service/resources/files-example/akmi.txt


poetry install;poetry build; docker rm -f packaging-service; docker rmi ekoindarto/packaging-service:0.1.5; docker build --no-cache -t ekoindarto/packaging-service:0.1.5 -f Dockerfile . ;docker run -d -p 2005:2005 --name packaging-service ekoindarto/packaging-service:0.1.5; docker exec -it packaging-service /bin/bash