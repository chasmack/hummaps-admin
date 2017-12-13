ssh -i %userprofile%/.aws/charlie-key-pair-uswest2.pem ubuntu@ec2.cmack.org sudo service apache2 stop
psql -h ec2.cmack.org -U ubuntu -d production -c "DROP SCHEMA hummaps CASCADE;"
psql -h ec2.cmack.org -U ubuntu -d production -f hummaps-dump.sql
ssh -i %userprofile%/.aws/charlie-key-pair-uswest2.pem ubuntu@ec2.cmack.org sudo service apache2 start
