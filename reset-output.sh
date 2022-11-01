#!/bin/bash

mkdir -p output/reports
mkdir -p output/dbms

rm -f  output/reports/*
rm -rf output/dbms/*

cd output/dbms
dolt sql --query "create database callrep"



