#!/bin/bash

DOMAIN=$1

wayback_machine_downloader 

tar zcf $DOMAIN.tar.gz
