#!/bin/bash

go build -race -buildmode=plugin ../mrapps/early_exit.go
go run -race mrworker.go early_exit.so
