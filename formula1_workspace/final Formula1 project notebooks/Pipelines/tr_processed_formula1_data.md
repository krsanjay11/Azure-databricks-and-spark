## Create trigger to run main pipeline
### manage > trigger > new
![image](https://github.com/krsanjay11/Azure-databricks-and-spark/assets/21271522/0bb6ec15-e1df-4884-aae4-9d4ee3346a76)

### Go to pipeline > edit or new and attach trigger
![image](https://github.com/krsanjay11/Azure-databricks-and-spark/assets/21271522/15593f1f-35bc-4db0-85ca-41fd5afe0b18)

### give run parameter @trigger().outputs.windowEndTime
![image](https://github.com/krsanjay11/Azure-databricks-and-spark/assets/21271522/931b2c10-76ac-4f0d-912a-72b9c33b5815)

### publish and run the trigger
