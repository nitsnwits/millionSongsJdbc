#!/bin/bash
#
#	Full Database Backup Script, Will take backup at 4:00 am
#	This script will be running in cron to take a full base backup at 4 am
#	This backup will serve as base backup of every day
#	Archive backup (WAL's) before this backup will not be needed then

#global variables
PGHOME=/Library/PostgreSQL/9.3
PGBIN=${PGHOME}/bin
PGDATA=${PGHOME}/data
PGARCHIVE=${PGHOME}/archives
PGFULLBACKUP=${PGHOME}/fullBackup
PGUser=postgres
PGPassword=data

#create file name based on timestamp
backupFile=`date +%Y-%m-%d_%H-%M`.tar.gz

#Sanity check for older backup
if [ -f /tmp/backup_in_progress ]
then
	echo "Previous backup was not done properly. Attempting to stop.."
	${PGBIN}/psql -U ${PGUser} -p ${PGPassword} -c "select pg_stop_backup();"
	rm /tmp/backup_in_progress
	echo "Please run the script again."
	exit 1
else
	echo "Previous backup check complete."
fi

#take base backup from pgsql credentials
echo "Starting full database backup at `date`.."
touch /tmp/backup_in_progress
${PGBIN}/psql -U ${PGUser} -p ${PGPassword} -c "select pg_start_backup('hot_backup');"
tar -czf ${PGFULLBACKUP}/${backupFile} ${PGDATA}
#gzip ${PGFULLBACKUP}/${backupFile}
${PGBIN}/psql -U ${PGUser} -p {PGPassword} -c "select pg_stop_backup();"
rm /tmp/backup_in_progress
echo "Full database backup complete. Backup present in ${PGFULLBACKUP}/${backupFile} ."
