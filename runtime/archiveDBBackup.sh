#!/bin/bash

#
#   This script is to take archive backups
#   This script will be scheduled in cron to run every 6 hours (configurable)
#   It will archive the archives and move it to tape (or another storage medium)
#   And remove the older archives to allow cushion for database directory
#

#global variables
PGHOME=/Library/PostgreSQL/9.3
PGBIN=${PGHOME}/bin
PGDATA=${PGHOME}/data
PGARCHIVE=${PGHOME}/archives
PGFULLBACKUP=${PGHOME}/fullBackup
PGUser=postgres
PGPassword=data
#configurable parameter to change storage type, giving another file system for now
PGArchiveStorage=${PGHOME}/archiveBackup

#create archive backup file based on timestamp
backupFile=`date +%Y-%m-%d_%H-%M`.tar

#Sanity check for older backup
if [ -f /tmp/archive_backup_in_progress ]
then
	echo "Previous archive backup was not done properly. Attempting to clear"
	count=`ps -ef|grep PostgreSQL|grep tar|grep -v grep|awk '{print $2}'|wc -l|awk '{print $1}'`
	if [ ${count} -eq 0 ]
	then
		rm /tmp/archive_backup_in_progress
		echo "Please run the script again."
	else
		echo "The process is still running. Kill the process and run the script again."
	fi
	exit 1
else
	echo "Previous backup check complete."
fi

#take archive backup from pgsql credentials
echo "Starting archive database backup at `date`.."
touch /tmp/archive_backup_in_progress
tar -cf ${PGHOME}/${backupFile} ${PGARCHIVE}
gzip ${PGHOME}/${backupFile}
if [ $? -eq 0 ]
	then
	mv ${PGHOME}/${backupFile}.gz ${PGArchiveStorage}
	rm -rf ${PGARCHIVE}/*
	echo "Archive Backup completed. Backup present in ${PGArchiveStorage}"
	rm /tmp/archive_backup_in_progress
else
	echo "Error creating backup."
fi




