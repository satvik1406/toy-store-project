### To run the code 

Update the `client.py` file in the client folder inside the part 1/2 (depending on what to test) with the server's IP address to ensure the client targets the correct server machine for communication.

If you want to deploy the changes on the EC2, the instructions to create and run the instance are in the Evaluation Document.

To run the server, go to each microservice folder and run the corresponding file inside it using the command: `python filename.py`
<br/> For example, frontend -> frontend_service.py <br/>
catalog -> catalog.py order -> order.py

If you aim to run a single client instance, run the client.py file in the client folder using the command : `python client.py`. To simulate multiple clients concurrently accessing the server, modify the script.sh file to specify the number of clients you wish to run parallely. Execute the script.sh script to start multiple clients as defined in the script configuration using the command : `./script.sh` 

Following these instructions will initiate the server and client(s), allowing you to evaluate the project's functionality and performance under various configurations and loads.