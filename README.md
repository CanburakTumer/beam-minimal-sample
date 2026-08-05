__Created by Antigravity__

# Apache Beam Minimal Sample Pipeline (Java + Gradle)

An Apache Beam pipeline written in **Java** using **Gradle**, configured for deployment via **Google Cloud Dataflow Flex Templates**.

The pipeline processes the first two paragraphs of Lorem Ipsum, counts the number of words starting with each letter (case-insensitive), and writes the results to a given GCS bucket partitioned as `YYYY/MM/DD/HH24:Mi:SS.yaml`.

## Output Format

Example path in GCS:
`gs://my-bucket/2026/08/05/15:30:00.yaml`

Content:
```yaml
A: 17
B: 1
C: 8
D: 13
E: 20
F: 2
I: 14
L: 6
M: 6
N: 10
O: 4
P: 4
Q: 12
R: 3
S: 12
T: 3
U: 7
V: 11
```

## Running Unit Tests

Unit tests use Apache Beam's `TestPipeline` and `PAssert` to verify the word count transformation:

```bash
gradle test
```

## Running Locally

Build the shaded JAR and run with the DirectRunner:

```bash
gradle shadowJar
java -cp build/libs/beam-minimal-sample.jar com.example.beam.LoremIpsumWordCountPipeline --outputBucket=/path/to/output
```

---

## Dataflow Flex Template Deployment

### 1. Build and Push Container Image to Artifact Registry / Container Registry

```bash
export PROJECT_ID="YOUR_GCP_PROJECT"
export REGION="us-central1"
export IMAGE_URL="${REGION}-docker.pkg.dev/${PROJECT_ID}/dataflow-templates/lorem-ipsum-count:latest"

gcloud auth configure-docker ${REGION}-docker.pkg.dev
docker build -t ${IMAGE_URL} .
docker push ${IMAGE_URL}
```

### 2. Build the Flex Template Spec File in GCS

```bash
export TEMPLATE_PATH="gs://YOUR_GCS_BUCKET/templates/lorem-ipsum-count.json"

gcloud dataflow flex-template build ${TEMPLATE_PATH} \
  --image "${IMAGE_URL}" \
  --sdk-language "JAVA" \
  --metadata-file "metadata.json"
```

### 3. Run the Flex Template Job

```bash
gcloud dataflow flex-template run "lorem-ipsum-word-count-$(date +%s)" \
  --template-file-gcs-location "${TEMPLATE_PATH}" \
  --region "${REGION}" \
  --parameters outputBucket="gs://YOUR_GCS_BUCKET"
```
