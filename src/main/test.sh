#!/bin/bash


sleep 30 &

sleep 30 &

sleep 30 &

echo $!

wait -n

echo "First job has been completed."

pwd

wait

echo "All jobs have been completed."
