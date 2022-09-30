# CF RSS

This version has a scheduler that makes an API request to Codeforces at fixed interval, retrieves the batch result, and stores it in MongoDB. This can retain the recent action history forever.

It also has a method to retrieves all the actions that happened after a fixed timestamp.

### Local Development
Make sure that you have `go` 1.18 installed. Also, MongoDB should be running on port `27017`.

If you don't have MongoDB running yet, use a docker image for experiments.

```shell
docker run -p 27017:27017 mongo
```

Or, if you want to run it as background service.

```shell
docker run -d -p 27017:27017 mongo
```

Now, from the application root, run

```shell
go run cmd/web/main.go
```

This should give you a fully configured environment, with the default flags. In case you want to customize it further, pass your own flags.


### Flags
* `--environment=dev` : If set to anything other than `dev`, the Zap logger would be created in production mode.
* `--mongo-addr=mongodb://localhost:27017` : Use this flag to provide the port on which MongoDB is running locally. If you want to connect to MongoDB Atlas (say, in production), use the format `mongodb+srv://admin:password@cluster0.s0g4l.mongodb.net/test`
* `--database-name=cfrss-local` : The database which stores the data. In production, set it to `cfrss`.
* `--cooldown-minutes=5` : The amount of time (in minutes) between successive Codeforces API calls.
* `--cf-batch-size=100` : The number of recent actions to retrieve in each Codeforces API call.

### Docker 
First, build the image using
```shell
docker build --tag cfrss:latest .
```

To run the docker image, you need to pass the MongoDB address as a flag. If you want to use the local instance of MongoDB, expose the network stack. 

```shell
docker run --network=host cfrss:latest 
```

If you want to use the cloud version, pass the flags like so

```shell
docker run -d --restart always -p 5000:5000 --name cfrss cfrss:latest --mongo-addr=mongodb+srv://admin:enterCorrectValue@cluster0.s0g4l.mongodb.net/test --database-name=cfrss-local --enable-cf-scheduler=false --environment=dev
```

In production, you need to change 3 things:
1. Enter the correct MongoDB credentials.
2. Enter the correct DB name: `cfrss`.
3. Set `enable-cf-scheduler` to `true`.
4. Set `environment` to `prod`.

To view the logs, 

```shell
docker logs cfrss
```

To stop the container
```shell
docker stop cfrss
```

It can be restarted via

```shell
docker start cfrss
```

To remove the container

```shell
docker rm cfrss
```