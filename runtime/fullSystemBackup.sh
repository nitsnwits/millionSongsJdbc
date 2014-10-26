#!/bin/bash
#
#	Full System Backup Script, Will take backup at 4:00 am
#
#

#global variables
PGHOME=/Library/PostgreSQL/9.3
PGBIN=${PGHOME}/bin
PGDATA=${PGHOME}/data
PGARCHIVE=${PGHOME}/archives
PGFULLBACKUP=${PGHOME}/fullBackup

#create file name based on timestamp
backupFile=`date +%Y-%m-%d_%H-%M`.tar

#Sanity check from older backup
if [ -f /tmp/backup_in_progress ]
then
	echo "Previous backup was not done properly. Please investigate"
	exit 1
else
	echo "Previous backup check complete."
fi
echo "Starting full system backup at `date`.."
touch /tmp/backup_in_progress
${PGBIN}/psql -u postgres -p data -c "select pg_start_backup('hot_backup');"
tar -cf ${PGFULLBACKUP}/${backupFile} ${PGDATA}
${PGBIN}/psql -u postgres -p data -c "select pg_stop_backup();"
rm /tmp/backup_in_progress
echo "Full system backup complete. Backup present in ${PGFULLBACKUP}/${backupFile} ."
