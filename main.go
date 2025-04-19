package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glacier"
	"github.com/aws/aws-sdk-go-v2/service/glacier/types"
)

func main() {
	// Take the following arguments
	//  --region   AWS region
	//  --profile  AWS shared config profile
	//  --vault    Vault name

	regionFlg := flag.String("region", "us-east-1", "AWS region")
	profileFlg := flag.String("profile", "default", "AWS shared config profile name")
	vaultFlg := flag.String("vault", "", "Vault name to empty")
	flag.Parse()
	
	// Validate flags
	if *vaultFlg == "" {
		log.Fatal("--vault must be set")
	}

	// Create the context
	ctx := context.Background()

	// Load configuration
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(*regionFlg),
		config.WithSharedConfigProfile(*profileFlg))
	if err != nil {
 		log.Fatalf("enable to load config: %v", err)
	}

	// Create the client
	glacierClient := glacier.NewFromConfig(cfg)

	// List running jobs
	listJobsOutput, err := glacierClient.ListJobs(ctx, &glacier.ListJobsInput{
		VaultName: vaultFlg,
	})
	if err != nil {
		log.Fatalf("unable to call ListJobs: %v", err);
	}

	// Look through all the jobs and find the latest completed job
	// If a job is still running, we should bail until it is completed
	//
	// TODO: Start a job if one is not running
	// TODO: If there is a job running, wait for it to complete
	var latestJob *string
	var latestCompletionDate *string
	
	for _, job := range listJobsOutput.JobList {
		if job.Action != types.ActionCodeInventoryRetrieval {
			continue
		}
		if !job.Completed {
			log.Fatalf("There is a running job with JobId %s", *job.JobId)
		}
		if latestCompletionDate == nil || *job.CompletionDate > *latestCompletionDate {
			latestJob = job.JobId
			latestCompletionDate = job.CompletionDate
		}
	}

	if latestJob == nil {
		log.Fatal("Unable to find a completed job")
	}

	// Read the output from the selected job
	getJobOutput, err := glacierClient.GetJobOutput(ctx, &glacier.GetJobOutputInput{
		VaultName: vaultFlg,
		JobId: latestJob ,
	})
	if err != nil {
		log.Fatalf("error getting job output: %v", err)
	}

	// Unmarshal the json
	outputJson, err := io.ReadAll(getJobOutput.Body)
	if err != nil {
		log.Fatalf("error reading output body: %v", err)
	}
	
	var outputData struct {
		VaultARN string
		InventoryDate *time.Time
		ArchiveList []struct {
			ArchiveId string
			ArchiveDescription string
			CreationDate *time.Time
			Size uint
			SHA256TreeHash string
		}
	}

	err = json.Unmarshal(outputJson, &outputData)
	if err != nil {
		log.Fatalf("error unmarhaling json: %v", err)
	}


	// Delete the archives
	for _, archive := range outputData.ArchiveList {
		_, err := glacierClient.DeleteArchive(ctx, &glacier.DeleteArchiveInput{
			ArchiveId: &archive.ArchiveId,
			VaultName: vaultFlg,
		})
		if err != nil {
			// Only log errors. Continue with deleting other archives
			log.Printf("failed to delete archive %s: %v\n", archive.ArchiveId, err)
		} else {
			log.Printf("deleted archive %s\n", archive.ArchiveId)
		}
		
	}

}
