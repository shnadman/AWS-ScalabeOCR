import software.amazon.awssdk.services.ec2.Ec2AsyncClient;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public final class EC2Methods {
    final static Ec2AsyncClient ec2Async = Ec2AsyncClient.create();
    final static Ec2Client ec2 = Ec2Client.create();

    public static String createManagerInstance()  {
        String amiId = "ami-03d41b286f5ba9655";

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.T2_MICRO)
                .maxCount(1)
                .minCount(1)
                .build();

        CompletableFuture<RunInstancesResponse> response = ec2Async.runInstances(runRequest);

        response.whenComplete((resp,err)->{
            try {
            String instanceId = resp.instances().get(0).instanceId();
            Tag tag = Tag.builder()
                    .key("Purpose")
                    .value("Manager")
                    .build();

            CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                    .resources(instanceId)
                    .tags(tag)
                    .build();

                    ec2Async.createTags(tagRequest);
                    System.out.printf(
                            "Successfully started EC2 Instance %s based on AMI %s",
                            instanceId, amiId);

            } catch (Ec2Exception e) {
                System.err.println(e.awsErrorDetails().errorMessage());
            }
        });

        return response.join().instances().get(0).instanceId();
    }

    public static void createWorkerInstances(int requiredWorkers, List<String> activeWorkers)  {

        String amiId = "ami-03d41b286f5ba9655";

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.T2_MICRO)
                .maxCount(requiredWorkers)
                .userData(getUserData("Worker"))
                .minCount(1)
                .build();

        CompletableFuture<RunInstancesResponse> response = ec2Async.runInstances(runRequest);

        response.whenComplete((resp,err)->{
            try {

                List<String> workerIds = resp.instances().parallelStream().map((Instance::instanceId)).collect(Collectors.toList());

                Tag tag = Tag.builder()
                        .key("Purpose")
                        .value("Worker")
                        .build();

                CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                        .resources(workerIds)
                        .tags(tag)
                        .build();

                ec2Async.createTags(tagRequest);

                activeWorkers.addAll(workerIds);
            } catch (Ec2Exception e) {
                System.err.println(e.awsErrorDetails().errorMessage());
            }
        });
    };

    public static void launchWorkerInstances(int requiredWorkers, List<String> activeWorkers)  {

        LaunchTemplateSpecification launchTemplateSpecs = LaunchTemplateSpecification.builder().launchTemplateName("Worker").build();

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .launchTemplate(launchTemplateSpecs)
                .maxCount(1)
                .minCount(1)
                .build();
        try {
            ec2.runInstances(runRequest);
        }
        catch (Ec2Exception e){
            e.printStackTrace();
        }
    };



    private static String getUserData(String purpose) {
        if(purpose.equals("Worker")) {
            String userData = "";
            userData = userData + "#!/bin/bash" + "\n";
            userData = userData + "sudo apt update" + "\n";
            userData = userData + "sudo apt install -y openjdk-11-jre openjdk-11-jdk awscli" + "\n";
            userData = userData + "aws s3 cp s3://dsp-ass1-na/Worker.jar Worker.jar" + "\n";
            userData = userData + "java -jar Worker.jar" + "\n";
            String base64UserData = null;
            base64UserData = new String(Base64.getEncoder().encode(userData.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
            return base64UserData;
        }
        else {
            String userData = "";
            userData = userData + "#!/bin/bash" + "\n";
            userData = userData + "sudo apt update" + "\n";
            userData = userData + "sudo apt install -y openjdk-11-jre openjdk-11-jdk awscli" + "\n";
            userData = userData + "aws s3 cp s3://dsp-ass1-na/Worker.jar Worker.jar" + "\n";
            userData = userData + "java -jar Worker.jar" + "\n";
            String base64UserData = null;
            base64UserData = new String(Base64.getEncoder().encode(userData.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
            return base64UserData;
        }
    }



    public static void terminateInstance(String instanceId){
        TerminateInstancesRequest request =TerminateInstancesRequest.builder().instanceIds(instanceId).build();
        CompletableFuture<TerminateInstancesResponse> future = ec2Async.terminateInstances(request);
        future.join();
    }

    public static void terminateInstances(List<String> instanceIds){
        TerminateInstancesRequest request =TerminateInstancesRequest.builder().instanceIds(instanceIds).build();
        CompletableFuture<TerminateInstancesResponse> future = ec2Async.terminateInstances(request);
        future.join();
    }



    public static String getActiveManagerID( ) {
        boolean done = false;
        String nextToken = null;

        try {
            do {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(6).nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);

                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        String state = instance.state().nameAsString();
                        String purpose = instance.tags().get(0).value();
                        if (purpose.equals("Manager") && (state.equals("running"))) {
                            return instance.instanceId();
                        }
                    }
                }
                nextToken = response.nextToken();
            } while (nextToken != null);

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return "None";
    }

}
