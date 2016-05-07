# Pipelines

The masterLambda is a general purpose lambda, which takes a video, chunks it, calls the WORKER LAMBDA on each chunk, gathers the result of all the WORKER LAMBDAS and calls the reducerLambda to join them. It then notifies completion on SNS.  

The reducerLambda, takes a list of S3 keys, downloads the keys, and joins them -if they are txt files then simply appends them to a long file. If they are video chunks it uses ffmpeg concat to rejoin them.  

The taggerLambda, imageclassifyLambda and the grayscaleLambdas are WORKER LAMBDAS, which take in an S3 key as input, operate on it, produce an output chunk or text, write it back to S3 and give its key as output.  
