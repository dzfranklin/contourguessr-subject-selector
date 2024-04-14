package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

var azureEndpoint string
var azureKey string
var targetCount int

func init() {
	err := godotenv.Load(".env", ".local.env")
	if err != nil {
		log.Fatal("Error loading .env file", err)
	}

	azureEndpoint = os.Getenv("AZURE_ENDPOINT")
	if azureEndpoint == "" {
		log.Fatal("AZURE_ENDPOINT not set")
	}

	azureKey = os.Getenv("AZURE_KEY")
	if azureKey == "" {
		log.Fatal("AZURE_KEY not set")
	}

	targetCountS := os.Getenv("TARGET_COUNT")
	if targetCountS == "" {
		log.Fatal("TARGET_COUNT not set")
	}
	targetCount, err = strconv.Atoi(targetCountS)
	if err != nil {
		log.Fatal("invalid TARGET_COUNT", err)
	}
}

func main() {
	manifestFiles, err := os.ReadDir("ingest_manifests")
	if err != nil {
		log.Fatal(err)
	}
	manifests := make(map[string][]ManifestEntry)
	for _, manifestFile := range manifestFiles {
		entries, err := parseManifestFile("ingest_manifests/" + manifestFile.Name())
		if err != nil {
			log.Fatal(err)
		}
		name := strings.TrimSuffix(manifestFile.Name(), ".json")
		manifests[name] = entries
	}

	if err := os.MkdirAll("analyses", 0750); err != nil {
		log.Fatal(err)
	}
	if err := os.MkdirAll("out", 0750); err != nil {
		log.Fatal(err)
	}

	for region, manifest := range manifests {
		processRegion(region, manifest)
	}
}

func processRegion(region string, manifest []ManifestEntry) {
	log.Printf("Processing region %s", region)

	preexistingFilename := "analyses/" + region + ".ndjson"
	preexisting := readPreexistingAnalyses(preexistingFilename)
	preexistingFile, err := os.OpenFile(preexistingFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0640)
	if err != nil {
		log.Fatal(err)
	}
	preexistingEnc := json.NewEncoder(preexistingFile)
	defer preexistingFile.Close()

	outFilename := "out/" + region + ".ndjson"
	outFile, err := os.Create(outFilename)
	if err != nil {
		log.Fatal(err)
	}
	outEnc := json.NewEncoder(outFile)
	defer outFile.Close()

	okCount := 0
	processedCount := 0
	apiCallCount := 0
	for _, entry := range manifest {
		if okCount >= targetCount {
			break
		}

		var picture ManifestEntry
		var analysis ImageAnalysis
		if existingEntry, ok := preexisting[entry.ID]; ok {
			picture = existingEntry.Picture
			analysis = existingEntry.Analysis
		} else {
			imageURL := flickrImagePreviewURL(entry)
			analysis = requestImageAnalysis(imageURL)
			picture = entry
			if err := preexistingEnc.Encode(AnalysisEntry{Picture: picture, Analysis: analysis}); err != nil {
				log.Fatal(err)
			}
			apiCallCount++
		}

		ok, issues := categorizeImage(analysis)
		webPreviewURL := flickrImageWebURL(entry)
		if ok {
			okCount++
			log.Printf("%d/%d OK %s %s", okCount, targetCount, webPreviewURL, entry.Title)
			if err := outEnc.Encode(picture.ID); err != nil {
				log.Fatal(err)
			}
		} else {
			log.Printf("%d/%d NG %s %s: %s", okCount, targetCount, webPreviewURL, entry.Title, issues)
		}

		processedCount++
	}

	log.Printf("Wrote %s", outFilename)
	log.Printf("Found %d after processing %d (%d API calls)", okCount, processedCount, apiCallCount)
}

func readPreexistingAnalyses(fname string) map[string]AnalysisEntry {
	existing := make(map[string]AnalysisEntry)
	analysesFile, err := os.Open(fname)
	if err != nil && !os.IsNotExist(err) {
		log.Fatal(err)
	}
	if err == nil {
		defer analysesFile.Close()
		dec := json.NewDecoder(analysesFile)
		for {
			var entry AnalysisEntry
			if err := dec.Decode(&entry); err == io.EOF {
				break
			} else if err != nil {
				log.Fatal(err)
			}
			existing[entry.Picture.ID] = entry
		}
		log.Printf("Read %d preexisting analyses from %s", len(existing), fname)
	}
	return existing
}

type AnalysisEntry struct {
	Picture  ManifestEntry `json:"picture"`
	Analysis ImageAnalysis `json:"analysis"`
}

func parseManifestFile(path string) ([]ManifestEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var entries []ManifestEntry
	if err := json.NewDecoder(f).Decode(&entries); err != nil {
		return nil, err
	}

	return entries, nil
}

type ManifestEntry struct {
	ID     string `json:"id"`
	Owner  string `json:"owner"`
	Secret string `json:"secret"`
	Server string `json:"server"`
	Title  string `json:"title"`
}

func categorizeImage(analysis ImageAnalysis) (bool, string) {
	var issues []string

	if analysis.Adult.IsAdultContent || analysis.Adult.IsRacyContent || analysis.Adult.IsGoryContent {
		issues = append(issues, "adult/racy/gory")
	}

	if analysis.Color.IsBWImg {
		issues = append(issues, "bw")
	}

	tags := make(map[string]float64)
	for _, tag := range analysis.Tags {
		tags[tag.Name] = tag.Confidence
	}

	if tags["outdoor"] < 0.8 || tags["nature"] < 0.8 {
		issues = append(issues, "!outdoor&&!nature")
	}
	if tags["mountain"] < 0.8 && tags["hill"] < 0.8 {
		issues = append(issues, "!mountain&&!hill")
	}
	if tags["sky"] < 0.8 && tags["landscape"] < 0.8 {
		issues = append(issues, "!sky&&!landscape")
	}

	imageArea := float64(analysis.Metadata.Width * analysis.Metadata.Height)
	objectsArea := float64(0)
	for _, obj := range analysis.Objects {
		objectsArea += float64(obj.Rectangle.W * obj.Rectangle.H)
	}
	objectPercentage := objectsArea / imageArea
	if objectPercentage > 0.2 {
		issues = append(issues, fmt.Sprintf("objects %.2f%%", objectPercentage*100))
	}

	return len(issues) == 0, strings.Join(issues, ",")
}

type ImageAnalysis struct {
	Adult struct {
		IsAdultContent bool `json:"isAdultContent"`
		IsRacyContent  bool `json:"isRacyContent"`
		IsGoryContent  bool `json:"isGoryContent"`
	} `json:"adult"`
	Color struct {
		IsBWImg bool `json:"isBWImg"`
	} `json:"color"`
	Tags []struct {
		Name       string  `json:"name"`
		Confidence float64 `json:"confidence"`
	} `json:"tags"`
	Objects []struct {
		Rectangle struct {
			X int `json:"x"`
			Y int `json:"y"`
			W int `json:"w"`
			H int `json:"h"`
		} `json:"rectangle"`
		Object     string  `json:"object"`
		Confidence float64 `json:"confidence"`
	} `json:"objects"`
	Metadata struct {
		Width  int    `json:"width"`
		Height int    `json:"height"`
		Format string `json:"format"`
	} `json:"metadata"`
}

type imageAnalysisRequestBody struct {
	URL string `json:"url"`
}

func requestImageAnalysis(imageURL string) ImageAnalysis {
	reqURL, err := url.Parse(azureEndpoint)
	if err != nil {
		log.Fatal(err)
	}

	reqURL.Path = "/vision/v3.1/analyze"

	params := map[string]string{
		"visualFeatures": "adult,color,tags,objects",
	}
	query := url.Values{}
	for k, v := range params {
		query.Set(k, v)
	}
	reqURL.RawQuery = query.Encode()

	body, err := json.Marshal(imageAnalysisRequestBody{URL: imageURL})

	req := http.Request{
		Method: "POST",
		URL:    reqURL,
		Header: http.Header{
			"Content-Type":              {"application/json"},
			"Ocp-Apim-Subscription-Key": {azureKey},
		},
		Body: io.NopCloser(bytes.NewReader(body)),
	}

	log.Printf("Calling Azure API: %s", strings.TrimPrefix(req.URL.String(), "https://"))

	httpResp, err := http.DefaultClient.Do(&req)
	if err != nil {
		log.Fatal(err)
	}
	if httpResp.StatusCode != http.StatusOK {
		log.Fatalf("Azure API HTTP status %d", httpResp.StatusCode)
	}
	defer httpResp.Body.Close()

	var analysis ImageAnalysis
	if err := json.NewDecoder(httpResp.Body).Decode(&analysis); err != nil {
		log.Fatal(err)
	}

	return analysis
}

func flickrImagePreviewURL(photo ManifestEntry) string {
	// https://live.staticflickr.com/{server-id}/{id}_{secret}_{size-suffix}.jpg
	return "https://live.staticflickr.com/" + photo.Server + "/" + photo.ID + "_" + photo.Secret + "_w.jpg"
}

func flickrImageWebURL(photo ManifestEntry) string {
	// https://www.flickr.com/photos/{owner-id}/{photo-id}
	return "https://www.flickr.com/photos/" + photo.Owner + "/" + photo.ID
}
