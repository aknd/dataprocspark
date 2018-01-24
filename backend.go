package backend

import (
    "errors"
    "fmt"
    "io/ioutil"
    "strings"
    "strconv"
    "time"
    "regexp"

    "net/http"
    "html/template"

    "golang.org/x/net/context"
    "golang.org/x/oauth2/google"

    "google.golang.org/api/iterator"

    "google.golang.org/appengine"
    "google.golang.org/appengine/log"
    "google.golang.org/appengine/taskqueue"

    storage "cloud.google.com/go/storage"
    dataproc "google.golang.org/api/dataproc/v1"

    "github.com/davecgh/go-spew/spew"
)

func init() {
    http.HandleFunc("/", home)
    http.HandleFunc("/enqueue", enqueue)
    http.HandleFunc("/pyspark_submit", pysparkSubmit)
    http.HandleFunc("/spark_submit", sparkSubmit)
    http.HandleFunc("/delete_clusters", deleteClusters)
    http.HandleFunc("/cleanup", cleanup)
}

var enqueueTemplate = template.Must(template.New("enqueue").Parse(enqueueHTML))

const (
    enqueueHTML = `{
    "projectID": "{{.ProjectID}}",
    "zoneID": "{{.ZoneID}}",
    "region": "{{.Region}}",
    "clusterName": "{{.ClusterName}}",
    "machineType": "{{.MachineType}}",
    "numWorkers": "{{.NumWorkers}}",
    "initializationAction": "{{.InitializationAction}}",
    "configBucket": "{{.ConfigBucket}}",
    "bucketName": "{{.BucketName}}",
    "args": "{{.Args}}",
    "pysparkFile": "{{.PysparkFile}}",
    "jarFile": "{{.JarFile}}",
    "mainClass": "{{.MainClass}}",
    "delete": "{{.Delete}}",
    "queueName": "{{.QueueName}}",
    "timeoutMin": "{{.TimeoutMin}}",
    "ETA": "{{.ETA}}"
}`

    location = "Asia/Tokyo"
    timestampFormat = "20060102150405"
    etaFormat = "2006-01-02 15:04:05.000000-07:00"

    zoneIDDefault = "asia-east1-a"
    regionDefault = "global"
    queueNameDefault = "default"
    timeoutMinDefault = 240

    initializationActionBucketDefault = "dataproc-initialization-actions"
    initializationActionFolderDefault = "conda"
    initializationActionFileDefault = "bootstrap-conda.sh"

    machineTypeDefault = "n1-standard-4"
    numWorkersDefault = 2

    cleanupDirDefault = "google-cloud-dataproc-metainfo"

    // Allows for full access to Google Cloud Platform products
    scope = "https://www.googleapis.com/auth/cloud-platform"

    // URI to Google Cloud Compute Engine instances within a zone, region, and project
    // https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.clusters
    computeURIFormat = "https://www.googleapis.com/compute/v1/projects/%s/zones/%s"
)

type QueueTask struct {
    ProjectID string
    ZoneID string
    Region string
    ClusterName string
    MachineType string
    NumWorkers string
    InitializationAction string
    ConfigBucket string
    BucketName string
    Args string
    PysparkFile string
    JarFile string
    MainClass string
    Delete string
    QueueName string
    TimeoutMin string
    ETA string
}

// clusterConfig defines the confuration of a cluster including its project, name, and region.
type clusterConfig struct {
    project string
    region  string
    name    string
    zone    string
    bucket string
    initializationActions []*dataproc.NodeInitializationAction
    masterConfig *dataproc.InstanceGroupConfig
    workerConfig *dataproc.InstanceGroupConfig
}

// clusterDetails defines the details of a created Cloud Dataproc cluster
type clusterDetails struct {
    bucket  string
    name    string
    project string
    region  string
    state   string
    uuid    string
    zone    string
}

func home(w http.ResponseWriter, r *http.Request) {
    fmt.Fprintf(w, "Home")
}

func enqueue(w http.ResponseWriter, r *http.Request) {
    ctx := appengine.NewContext(r)

    loc, _ := time.LoadLocation(location)
    jst := time.Now().In(loc)
    timestamp := jst.Format(timestampFormat)
    eta := jst.Format(etaFormat)

    projectID := r.FormValue("project_id")
    if projectID == "" {
        log.Errorf(ctx, "%v", "Required parameter 'project_id' is missing.")
        panic("Enqueue failed.")
    }

    zoneID := r.FormValue("zone")
    if zoneID == "" {
        zoneID = zoneIDDefault
    }

    region := r.FormValue("region")
    if region == "" {
        region = regionDefault
    }

    machineType := r.FormValue("machine_type")
    if machineType == "" {
        machineType = machineTypeDefault
    }

    numWorkers := r.FormValue("num_workers")
    if numWorkers == "" {
        numWorkers = strconv.Itoa(numWorkersDefault)
    }

    initializationActionBucket := r.FormValue("initialization_action_bucket")
    if initializationActionBucket == "" {
        initializationActionBucket = initializationActionBucketDefault
    }
    initializationActionFolder := r.FormValue("initialization_action_folder")
    if initializationActionFolder == "" {
        initializationActionFolder = initializationActionFolderDefault
    }
    initializationActionFile := r.FormValue("initialization_action_file")
    if initializationActionFile == "" {
        initializationActionFile = initializationActionFileDefault
    }
    initializationAction := "gs://" + initializationActionBucket + "/" + initializationActionFolder + "/" + initializationActionFile

    configBucket := r.FormValue("config_bucket")
    if configBucket == "" {
        log.Errorf(ctx, "%v", "Required parameter 'config_bucket' is missing.")
        panic("Enqueue failed.")
    }

    bucketName := r.FormValue("bucket_name")
    if bucketName == "" {
        log.Errorf(ctx, "%v", "Required parameter 'bucket_name' is missing.")
        panic("Enqueue failed.")
    }

    args:= r.FormValue("args")

    delete := r.FormValue("delete")
    if delete == "" {
        delete = "1"
    }

    queueName := r.FormValue("queue_name")
    if queueName == "" {
        queueName = queueNameDefault
    }

    timeoutMin := r.FormValue("timeout_min")
    if timeoutMin == "" {
        timeoutMin = strconv.Itoa(timeoutMinDefault)
    }

    var mainFile string
    clusterName := r.FormValue("cluster_name")
    var postPath string
    task := QueueTask{}
    params := map[string][]string{}

    mainClass := r.FormValue("main_class")
    if mainClass == "" {
        mainFile = r.FormValue("pyspark_file")
        if mainFile == "" {
            if r.FormValue("jar_file") == "" {
              log.Errorf(ctx, "%v", "Required parameter 'pyspark_file' is missing.")
            } else {
              log.Errorf(ctx, "%v", "Required parameter 'main_class' is missing.")
            }
            return
        }
        mainFile = "src/main/python/" + strings.Replace(mainFile, "\\/", "/", -1)

        if clusterName == "" {
            dirs := strings.Split(mainFile, "/")
            rep := regexp.MustCompile(`\.py$`)
            clusterNameRaw := rep.ReplaceAllString(dirs[len(dirs) - 1], "")
            rep = regexp.MustCompile(`[\W_]+`)
            clusterNameRaw = rep.ReplaceAllString(clusterNameRaw, "-")
            clusterName = clusterNameRaw + "-" + timestamp
        }

        postPath = "/pyspark_submit"

        task = QueueTask{
            ProjectID: projectID,
            ZoneID: zoneID,
            Region: region,
            ClusterName: clusterName,
            MachineType: machineType,
            NumWorkers: numWorkers,
            InitializationAction: initializationAction,
            ConfigBucket: configBucket,
            BucketName: bucketName,
            Args: args,
            PysparkFile: mainFile,
            Delete: delete,
            QueueName: queueName,
            TimeoutMin: timeoutMin,
            ETA: eta,
        }

        params = map[string][]string{
            "projectID": {task.ProjectID},
            "zoneID": {task.ZoneID},
            "region": {task.Region},
            "clusterName": {task.ClusterName},
            "machineType": {task.MachineType},
            "numWorkers": {task.NumWorkers},
            "initializationAction": {task.InitializationAction},
            "configBucket": {task.ConfigBucket},
            "bucketName": {task.BucketName},
            "args": {task.Args},
            "pysparkFile": {task.PysparkFile},
            "delete": {task.Delete},
            "timeoutMin": {task.TimeoutMin},
        }
    } else {
        mainFile = r.FormValue("jar_file")
        if mainFile == "" {
            log.Errorf(ctx, "%v", "Required parameter 'jar_file' is missing.")
            panic("Enqueue failed.")
        }
        mainFile = "target/scala-2.11/" + mainFile

        if clusterName == "" {
            nsps := strings.Split(mainClass, ".")
            clusterNameRaw := nsps[len(nsps) - 1]
            rep := regexp.MustCompile(`[\W_]+`)
            clusterNameRaw = rep.ReplaceAllString(ToSnakeCase(clusterNameRaw), "-")
            clusterName = clusterNameRaw + "-" + timestamp
        }

        postPath = "/spark_submit"

        task = QueueTask{
            ProjectID: projectID,
            ZoneID: zoneID,
            Region: region,
            ClusterName: clusterName,
            MachineType: machineType,
            NumWorkers: numWorkers,
            InitializationAction: initializationAction,
            ConfigBucket: configBucket,
            BucketName: bucketName,
            Args: args,
            JarFile: mainFile,
            MainClass: mainClass,
            Delete: delete,
            QueueName: queueName,
            TimeoutMin: timeoutMin,
            ETA: eta,
        }

        params = map[string][]string{
            "projectID": {task.ProjectID},
            "zoneID": {task.ZoneID},
            "region": {task.Region},
            "clusterName": {task.ClusterName},
            "machineType": {task.MachineType},
            "numWorkers": {task.NumWorkers},
            "initializationAction": {task.InitializationAction},
            "configBucket": {task.ConfigBucket},
            "bucketName": {task.BucketName},
            "args": {task.Args},
            "jarFile": {task.JarFile},
            "mainClass": {task.MainClass},
            "delete": {task.Delete},
            "timeoutMin": {task.TimeoutMin},
        }
    }

    t := taskqueue.NewPOSTTask(postPath, params)
    if _, err := taskqueue.Add(ctx, t, queueName); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    if err := enqueueTemplate.Execute(w, task); err != nil {
        taskStr := spew.Sdump(task)
        log.Errorf(ctx, "%v", taskStr)
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
}

func pysparkSubmit(w http.ResponseWriter, r *http.Request) {
    ctx := appengine.NewContext(r)

    projectID := r.FormValue("projectID")
    zoneID := r.FormValue("zoneID")
    region := r.FormValue("region")
    clusterName := r.FormValue("clusterName")
    machineType := r.FormValue("machineType")
    numWorkers, _ := strconv.ParseInt(r.FormValue("numWorkers"), 10, 0)
    initializationAction := r.FormValue("initializationAction")
    configBucket := r.FormValue("configBucket")
    bucketName := r.FormValue("bucketName")
    args := r.FormValue("args")
    pysparkFile := r.FormValue("pysparkFile")
    timeoutMin, _ := strconv.ParseInt(r.FormValue("timeoutMin"), 10, 0)

    if projectID == "" || zoneID == "" || region == "" || clusterName == "" || machineType == "" || numWorkers == 0 || initializationAction == "" || configBucket == "" || bucketName == "" || pysparkFile == "" || timeoutMin == 0 {
        log.Errorf(ctx, "%v", "Parameters incorrect")
    }

    var delete bool
    if d := strings.ToLower(r.FormValue("delete")); d == "0" || d == "f" || d == "false" {
        delete = false
    } else {
        delete = true
    }

    log.Infof(ctx, "projectID: %v", projectID)
    log.Infof(ctx, "zoneID: %v", zoneID)
    log.Infof(ctx, "region: %v", region)
    log.Infof(ctx, "clusterName: %v", clusterName)
    log.Infof(ctx, "machineType: %v", machineType)
    log.Infof(ctx, "numWorkers: %v", numWorkers)
    log.Infof(ctx, "initializationAction: %v", initializationAction)
    log.Infof(ctx, "configBucket: %v", configBucket)
    log.Infof(ctx, "bucketName: %v", bucketName)
    log.Infof(ctx, "args: %v", args)
    log.Infof(ctx, "pysparkFile: %v", pysparkFile)
    log.Infof(ctx, "delete: %v", delete)
    log.Infof(ctx, "timeoutMin: %v", timeoutMin)

    client, err := google.DefaultClient(ctx, scope)
    if err != nil {
        log.Errorf(ctx, "%v", err)
    }
    service, err := dataproc.New(client)
    if err != nil {
        log.Errorf(ctx, "%v", err)
    }

    storageClient, err := storage.NewClient(ctx)
    if err != nil {
        log.Errorf(ctx, "%v", err)
    }

    machineTypeUri := "https://www.googleapis.com/compute/v1/projects/" + projectID + "/zones/" + zoneID + "/machineTypes/" + machineType

    // Create a new clusterConfig to hold details about this cluster
    configuration := clusterConfig{project: projectID, region: region, name: clusterName, zone: zoneID, bucket: configBucket, initializationActions: []*dataproc.NodeInitializationAction{&dataproc.NodeInitializationAction{ExecutableFile: initializationAction, ExecutionTimeout: "1200s"}}, masterConfig: &dataproc.InstanceGroupConfig{MachineTypeUri: machineTypeUri}, workerConfig: &dataproc.InstanceGroupConfig{MachineTypeUri: machineTypeUri, NumInstances: numWorkers}}

    // Create a cluster if not exists
    if err := createClusterIfNil(service, configuration, r); err != nil {
        log.Errorf(ctx, "%v", err)
        panic("Cluster creation failed.")
    }

    // Get the cluster's details
    cluster, err := getClusterDetails(service, configuration)
    if err != nil {
        log.Errorf(ctx, "%v", err)
        if _, e := deleteCluster(service, cluster); e != nil {
            log.Errorf(ctx, "%v", e)
            panic("Cluster deletion failed.")
        }
        log.Infof(ctx, "%v", "Cluster deleted")
        panic("Unable to get cluster details.")
    }

    // Submit a job to the cluster
    log.Infof(ctx, "%v", "Submitting job")
    jobID, err := submitPySparkJob(service, storageClient, pysparkFile, args, bucketName, cluster)
    if err != nil {
        log.Errorf(ctx, "%v", err)
        if _, e := deleteCluster(service, cluster); e != nil {
            log.Errorf(ctx, "%v", e)
            panic("Cluster deletion failed.")
        }
        log.Infof(ctx, "%v", "Cluster deleted")
        panic("Job submission failed.")
    }

    // Wait for the job to complete
    log.Infof(ctx, "%v", "Waiting for job to complete")
    finished, jobErr := waitForJob(service, jobID, cluster, int(timeoutMin), r)
    if jobErr != nil {
        log.Errorf(ctx, "%v", jobErr)
    }
    if !finished {
        if _, err := deleteCluster(service, cluster); err != nil {
            log.Errorf(ctx, "%v", err)
            panic("Cluster deletion failed.")
        }
        log.Infof(ctx, "%v", "Cluster deleted")
        panic("Job execution failed.")
    }
    log.Infof(ctx, "%v", "Job is finished")

    output, err := getJobOutput(storageClient, jobID, cluster)
    if err != nil {
        log.Errorf(ctx, "%v", err)
        if _, e := deleteCluster(service, cluster); e != nil {
            log.Errorf(ctx, "%v", e)
            panic("Cluster deletion failed.")
        }
        log.Infof(ctx, "%v", "Cluster deleted")
        panic("Unable to get job output.")
    }

    log.Infof(ctx, "cluster.uuid: %v", cluster.uuid)
    log.Infof(ctx, "jobID: %v", jobID)
    log.Infof(ctx, "Job output:\n%v\n", string(output))

    // Delete the cluster
    if delete {
        if _, err := deleteCluster(service, cluster); err != nil {
            log.Errorf(ctx, "%v", err)
            panic("Cluster deletion failed.")
        }
        log.Infof(ctx, "%v", "Cluster deleted")
    }
}

func sparkSubmit(w http.ResponseWriter, r *http.Request) {
    ctx := appengine.NewContext(r)

    projectID := r.FormValue("projectID")
    zoneID := r.FormValue("zoneID")
    region := r.FormValue("region")
    clusterName := r.FormValue("clusterName")
    machineType := r.FormValue("machineType")
    numWorkers, _ := strconv.ParseInt(r.FormValue("numWorkers"), 10, 0)
    initializationAction := r.FormValue("initializationAction")
    configBucket := r.FormValue("configBucket")
    bucketName := r.FormValue("bucketName")
    args := r.FormValue("args")
    jarFile := r.FormValue("jarFile")
    mainClass := r.FormValue("mainClass")
    timeoutMin, _ := strconv.ParseInt(r.FormValue("timeoutMin"), 10, 0)

    if projectID == "" || zoneID == "" || region == "" || clusterName == "" || machineType == "" || numWorkers == 0 || initializationAction == "" || configBucket == "" || bucketName == "" || jarFile == "" || mainClass == "" || timeoutMin == 0 {
        log.Errorf(ctx, "%v", "Parameters incorrect")
    }

    var delete bool
    if d := strings.ToLower(r.FormValue("delete")); d == "0" || d == "f" || d == "false" {
        delete = false
    } else {
        delete = true
    }

    log.Infof(ctx, "projectID: %v", projectID)
    log.Infof(ctx, "zoneID: %v", zoneID)
    log.Infof(ctx, "region: %v", region)
    log.Infof(ctx, "clusterName: %v", clusterName)
    log.Infof(ctx, "machineType: %v", machineType)
    log.Infof(ctx, "numWorkers: %v", numWorkers)
    log.Infof(ctx, "initializationAction: %v", initializationAction)
    log.Infof(ctx, "configBucket: %v", configBucket)
    log.Infof(ctx, "bucketName: %v", bucketName)
    log.Infof(ctx, "args: %v", args)
    log.Infof(ctx, "jarFile: %v", jarFile)
    log.Infof(ctx, "mainClass: %v", mainClass)
    log.Infof(ctx, "delete: %v", delete)
    log.Infof(ctx, "timeoutMin: %v", timeoutMin)

    client, err := google.DefaultClient(ctx, scope)
    if err != nil {
        log.Errorf(ctx, "%v", err)
    }
    service, err := dataproc.New(client)
    if err != nil {
        log.Errorf(ctx, "%v", err)
    }

    storageClient, err := storage.NewClient(ctx)
    if err != nil {
        log.Errorf(ctx, "%v", err)
    }

    machineTypeUri := "https://www.googleapis.com/compute/v1/projects/" + projectID + "/zones/" + zoneID + "/machineTypes/" + machineType

    // Create a new clusterConfig to hold details about this cluster
    configuration := clusterConfig{project: projectID, region: region, name: clusterName, zone: zoneID, bucket: configBucket, initializationActions: []*dataproc.NodeInitializationAction{&dataproc.NodeInitializationAction{ExecutableFile: initializationAction, ExecutionTimeout: "1200s"}}, masterConfig: &dataproc.InstanceGroupConfig{MachineTypeUri: machineTypeUri}, workerConfig: &dataproc.InstanceGroupConfig{MachineTypeUri: machineTypeUri, NumInstances: numWorkers}}

    // Create a cluster if not exists
    if err := createClusterIfNil(service, configuration, r); err != nil {
        log.Errorf(ctx, "%v", err)
        panic("Cluster creation failed.")
    }
    log.Infof(ctx, "%v", "Cluster created")

    // Get the cluster's details
    cluster, err := getClusterDetails(service, configuration)
    if err != nil {
        log.Errorf(ctx, "%v", err)
        if _, e := deleteCluster(service, cluster); e != nil {
            log.Errorf(ctx, "%v", e)
            panic("Cluster deletion failed.")
        }
        log.Infof(ctx, "%v", "Cluster deleted")
        panic("Unable to get cluster details.")
    }

    // Submit a job to the cluster
    log.Infof(ctx, "%v", "Submitting job")
    jobID, err := submitSparkJob(service, storageClient, jarFile, mainClass, args, bucketName, cluster)
    if err != nil {
        log.Errorf(ctx, "%v", err)
        if _, e := deleteCluster(service, cluster); e != nil {
            log.Errorf(ctx, "%v", e)
            panic("Cluster deletion failed.")
        }
        log.Infof(ctx, "%v", "Cluster deleted")
        panic("Job submission failed.")
    }

    // Wait for the job to complete
    log.Infof(ctx, "%v", "Waiting for job to complete")
    finished, jobErr := waitForJob(service, jobID, cluster, int(timeoutMin), r)
    if jobErr != nil {
        log.Errorf(ctx, "%v", jobErr)
    }
    if !finished {
        if _, err := deleteCluster(service, cluster); err != nil {
            log.Errorf(ctx, "%v", err)
            panic("Cluster deletion failed.")
        }
        log.Infof(ctx, "%v", "Cluster deleted")
        panic("Job execution failed.")
    }
    log.Infof(ctx, "%v", "Job is finished")

    output, err := getJobOutput(storageClient, jobID, cluster)
    if err != nil {
        log.Errorf(ctx, "%v", err)
        if _, e := deleteCluster(service, cluster); e != nil {
            log.Errorf(ctx, "%v", e)
            panic("Cluster deletion failed.")
        }
        log.Infof(ctx, "%v", "Cluster deleted")
        panic("Unable to get job output.")
    }

    log.Infof(ctx, "cluster.uuid: %v", cluster.uuid)
    log.Infof(ctx, "jobID: %v", jobID)
    log.Infof(ctx, "Job output:\n%v\n", string(output))

    // Delete the cluster
    if delete {
        if _, err := deleteCluster(service, cluster); err != nil {
            log.Errorf(ctx, "%v", err)
            panic("Cluster deletion failed.")
        }
        log.Infof(ctx, "%v", "Cluster deleted")
    }
}

// createClusterIfNil creates a Cloud Dataproc cluster with the given name and region if it does not exists.
func createClusterIfNil(service *dataproc.Service, cluster clusterConfig, r *http.Request) (err error) {
    ctx := appengine.NewContext(r)

    // Create a gceConfig object for the cluster
    var scopes []string
    scopes = append(scopes, scope)
    gceConfig := dataproc.GceClusterConfig{
        ZoneUri:fmt.Sprintf(computeURIFormat, cluster.project, cluster.zone),
        ServiceAccountScopes: scopes,
    }

    // Create a (Dataproc API) clusterConfig for the cluster
    clusterConfig := dataproc.ClusterConfig{
        GceClusterConfig: &gceConfig,
        ConfigBucket: cluster.bucket,
        InitializationActions: cluster.initializationActions,
        MasterConfig: cluster.masterConfig,
        WorkerConfig: cluster.workerConfig,
    }

    // Create a cluster object
    clusterSpec := dataproc.Cluster{
        ClusterName: cluster.name,
        ProjectId:   cluster.project,
        Config:      &clusterConfig,
    }

    if cl, err := service.Projects.Regions.Clusters.Get(cluster.project, cluster.region, cluster.name).Do(); cl != nil {
        return err
    }

    // Create the cluster
    if _, err = service.Projects.Regions.Clusters.Create(cluster.project, cluster.region, &clusterSpec).Do(); err != nil {
        return err
    }
    log.Infof(ctx, "%v", "Cluster created")

    log.Infof(ctx, "%v", "Waiting for cluster to be ready")
    _, err = waitForCluster(service, cluster)
    if err != nil {
        log.Errorf(ctx, "%v", err)
    }
    return err
}

// deleteCluster deletes the Cloud Dataproc cluster with the given project, region, and name.
func deleteCluster(service *dataproc.Service, cluster clusterDetails) (response string, err error) {
    res, err := service.Projects.Regions.Clusters.Delete(cluster.project, cluster.region, cluster.name).Do()

    return fmt.Sprintf("%s", res), err
}

// getJobOutput returns the text from the job (raw driver output) with the given project, cluser name, bucket id, and job id.
func getJobOutput(storageClient *storage.Client, job string, cluster clusterDetails) (output []byte, err error) {
    // Format the object name based on the Cloud Dataproc service's GCS logging
    // see https://cloud.google.com/dataproc/concepts/driver-output for details
    object := fmt.Sprintf("google-cloud-dataproc-metainfo/%s/jobs/%s/driveroutput.000000000", cluster.uuid, job)

    // Read the file
    rc, err := storageClient.Bucket(cluster.bucket).Object(object).NewReader(context.Background())
    output, err = ioutil.ReadAll(rc)
    rc.Close()

    return output, err
}

// getJobStatusState returns the state of the job with the given job id, project, and region.
func getJobStatusState(service *dataproc.Service, jobID string, project string, region string) (state string, err error) {
    // Get the Job's status
    res, err := service.Projects.Regions.Jobs.Get(project, region, jobID).Do()
    if err != nil {
        return "", err
    }
    return res.Status.State, nil
}

// getJobStatusStateStartTime returns the start time of current state of the job with the given job id, project, and region.
func getJobStatusStateStartTime(service *dataproc.Service, jobID string, project string, region string) (stateStartTime string, err error) {
    // Get the Job's status
    res, err := service.Projects.Regions.Jobs.Get(project, region, jobID).Do()
    if err != nil {
        return "", err
    }
    return res.Status.StateStartTime, nil
}

// listClusters lists all clusters in the current project.
func listClusters(service *dataproc.Service, project string, region string) (clusters []clusterDetails, err error) {
    // List all clusters in a project for a given region
    res, err := service.Projects.Regions.Clusters.List(project, region).Do()
    if err != nil {
        return nil, err
    }

    for _, c := range res.Clusters {
        regionURIParts := strings.Split(c.Config.GceClusterConfig.NetworkUri, "/")
        region := regionURIParts[len(regionURIParts)-3]
        zoneURIParts := strings.Split(c.Config.GceClusterConfig.ZoneUri, "/")
        zoneID := zoneURIParts[len(zoneURIParts)-1]
        details := clusterDetails{
            bucket:  c.Config.ConfigBucket,
            name:    c.ClusterName,
            uuid:    c.ClusterUuid,
            project: c.ProjectId,
            region:  region,
            state:   c.Status.State,
            zone:    zoneID}
        clusters = append(clusters, details)
    }

    return clusters, err
}

// submitPySparkJob submits a PySpark job with the given file path to a PySpark file, project, bucket, and cluster.
func submitPySparkJob(service *dataproc.Service, storageClient *storage.Client, filepath string, args string, bucket string, cluster clusterDetails) (jobID string, err error) {
    // Submit the PySpark job
    placement := dataproc.JobPlacement{
        ClusterName: cluster.name,
    }
    pySparkJob := dataproc.PySparkJob{
        MainPythonFileUri: "gs://" + bucket + "/" + filepath,
        Args: []string{args},
    }
    loc, _ := time.LoadLocation(location)
    jst := time.Now().In(loc)
    timestamp := jst.Format(timestampFormat)
    jobID = "pyspark-job-" + fmt.Sprintf("%v", timestamp)
    jobReference := dataproc.JobReference{
        JobId:     jobID,
        ProjectId: cluster.project,
    }
    job := dataproc.Job{
        Placement:  &placement,
        PysparkJob: &pySparkJob,
        Reference:  &jobReference,
    }
    jobRequest := dataproc.SubmitJobRequest{
        Job: &job,
    }

    _, err = service.Projects.Regions.Jobs.Submit(cluster.project, cluster.region, &jobRequest).Do()
    return jobID, err
}

// submitSparkJob submits a Spark job with the given file path to a JAR file, main class, project, bucket, and cluster.
func submitSparkJob(service *dataproc.Service, storageClient *storage.Client, filepath string, mainClass string, args string, bucket string, cluster clusterDetails) (jobID string, err error) {
    // Submit the Spark job
    placement := dataproc.JobPlacement{
        ClusterName: cluster.name,
    }
    sparkJob := dataproc.SparkJob{
        JarFileUris: []string{"gs://" + bucket + "/" + filepath},
        MainClass: mainClass,
        Args: []string{args},
    }
    loc, _ := time.LoadLocation(location)
    jst := time.Now().In(loc)
    timestamp := jst.Format(timestampFormat)
    jobID = "spark-job-" + fmt.Sprintf("%v", timestamp)
    jobReference := dataproc.JobReference{
        JobId:     jobID,
        ProjectId: cluster.project,
    }
    job := dataproc.Job{
        Placement:  &placement,
        SparkJob: &sparkJob,
        Reference:  &jobReference,
    }
    jobRequest := dataproc.SubmitJobRequest{
        Job: &job,
    }

    _, err = service.Projects.Regions.Jobs.Submit(cluster.project, cluster.region, &jobRequest).Do()
    return jobID, err
}

// getClusterDetails returns details about a cluster with the specified cluster config.
func getClusterDetails(service *dataproc.Service, cluster clusterConfig) (details clusterDetails, err error) {
    clusters, err := listClusters(service, cluster.project, cluster.region)
    if err != nil {
        return details, err
    }

    // Find the cluster requested in the list
    for _, c := range clusters {
        if c.name == cluster.name {
            return c, nil
        }
    }

    return details, errors.New("cluster not found")
}

// waitForCluster waits for a cluster transition from "starting" to "running" with the given name.
func waitForCluster(service *dataproc.Service, cluster clusterConfig) (running bool, err error) {
    for {
        details, err := getClusterDetails(service, cluster)
        if err != nil {
            return false, err
        }
        if details.state == "RUNNING" {
            return true, nil
        }

        // Sleep for one second
        time.Sleep(1000 * time.Millisecond)
    }
}

// waitForJob waits for a job to finish with the given job id, project, and region.
func waitForJob(service *dataproc.Service, jobID string, cluster clusterDetails, timeoutMin int, r *http.Request) (finished bool, err error) {
    ctx := appengine.NewContext(r)

    state := ""

    for {
        jobStatusState, err := getJobStatusState(service, jobID, cluster.project, cluster.region)
        if err != nil {
            return false, err
        }
        jobStatusStateStartTime, err := getJobStatusStateStartTime(service, jobID, cluster.project, cluster.region)
        startTime, _ := time.Parse("2006-01-02T15:04:05.000Z", jobStatusStateStartTime)
        startTimeJst := startTime.In(time.FixedZone(location, 9*60*60))
        if err != nil {
            return false, err
        }
        if jobStatusState != state {
            state = jobStatusState
            log.Infof(ctx, "State: %v", jobStatusState)
            log.Infof(ctx, "StateStartTime: %v", startTimeJst)
        }
        if jobStatusState == "DONE" {
            return true, nil
        } else if jobStatusState == "ERROR" {
            return true, errors.New("Job errored")
        }
        loc, _ := time.LoadLocation(location)
        jst := time.Now().In(loc)
        duration := jst.Sub(startTimeJst)
        durationMin := int(duration.Minutes()) % 60
        if durationMin >= timeoutMin {
            log.Infof(ctx, "%v minutes have passed since the start of the state.", durationMin)
            log.Errorf(ctx, "%v", "Timeout error.")
            return false, nil
        }

        time.Sleep(time.Second)
    }

    return finished, err
}

func deleteClusters(w http.ResponseWriter, r *http.Request) {
    ctx := appengine.NewContext(r)

    client, err := google.DefaultClient(ctx, scope)
    if err != nil {
        log.Errorf(ctx, "%v", err)
    }
    service, err := dataproc.New(client)
    if err != nil {
        log.Errorf(ctx, "%v", err)
    }
    projectID := r.FormValue("project_id")
    if projectID == "" {
        log.Errorf(ctx, "%v", "Required parameter 'project_id' is missing.")
        panic("Enqueue failed.")
    }
    region := r.FormValue("region")
    if region == "" {
        region = regionDefault
    }

    clusterDetails, err := listClusters(service, projectID, region)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    for _, clusterDetail := range clusterDetails {
        deleteCluster(service, clusterDetail)
    }
}

func cleanup(w http.ResponseWriter, r *http.Request) {
    ctx := appengine.NewContext(r)

    bucketName := r.FormValue("bucket_name")
    if bucketName == "" {
        log.Errorf(ctx, "%v", "Required parameter 'bucket_name' is missing.")
        panic("Enqueue failed.")
    }

    cleanupDir := r.FormValue("cleanup_dir")
    if cleanupDir == "" {
        cleanupDir = cleanupDirDefault
    }

    client, err := storage.NewClient(ctx)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    objNames, err := getStrageObjectNamesByPrefix(client, bucketName, cleanupDir, "", r)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    for _, objName := range objNames {
        deleteStrageObject(client, bucketName, objName, r)
    }
}

func getStrageObjectNamesByPrefix(client *storage.Client, bucket string, prefix string, delim string, r *http.Request) (objNames []string, err error) {
    ctx := appengine.NewContext(r)

    it := client.Bucket(bucket).Objects(ctx, &storage.Query{
        Prefix: prefix,
        Delimiter: delim,
    })
    for {
        attrs, err := it.Next()
        if err == iterator.Done {
            break
        }
        if err != nil {
            return objNames, err
        }
        objNames = append(objNames, attrs.Name)
    }

    return objNames, nil
}

func deleteStrageObject(client *storage.Client, bucket string, object string, r *http.Request) error {
    ctx := appengine.NewContext(r)

    o := client.Bucket(bucket).Object(object)
    if err := o.Delete(ctx); err != nil {
        return err
    }
    log.Infof(ctx, "Strage object \"%v\" deleted.", object)

    return nil
}

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

func ToSnakeCase(str string) string {
    snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
    snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
    return strings.ToLower(snake)
}
