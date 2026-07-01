package metadata

import (
	"net/http"
)

func documentArtifactRouteNotImplemented(w http.ResponseWriter) {
	http.Error(w, "document artifact metadata route is not implemented", http.StatusNotImplemented)
}

func (t *TableApi) StartDocumentArtifactReprocessJob(w http.ResponseWriter, r *http.Request, tableName string, artifactName string) {
	documentArtifactRouteNotImplemented(w)
}

func (t *TableApi) GetDocumentArtifactReprocessJob(w http.ResponseWriter, r *http.Request, tableName string, artifactName string, jobId string) {
	documentArtifactRouteNotImplemented(w)
}

func (t *TableApi) AdvanceDocumentArtifactReprocessJob(w http.ResponseWriter, r *http.Request, tableName string, artifactName string, jobId string) {
	documentArtifactRouteNotImplemented(w)
}

func (t *TableApi) CancelDocumentArtifactReprocessJob(w http.ResponseWriter, r *http.Request, tableName string, artifactName string, jobId string) {
	documentArtifactRouteNotImplemented(w)
}

func (t *TableApi) ReprocessDocumentArtifactRange(w http.ResponseWriter, r *http.Request, tableName string, artifactName string) {
	documentArtifactRouteNotImplemented(w)
}

func (t *TableApi) ListArtifactEnrichments(w http.ResponseWriter, r *http.Request, tableName string) {
	documentArtifactRouteNotImplemented(w)
}

func (t *TableApi) PutArtifactEnrichment(w http.ResponseWriter, r *http.Request, tableName string, artifactName string) {
	documentArtifactRouteNotImplemented(w)
}

func (t *TableApi) DeleteArtifactEnrichment(w http.ResponseWriter, r *http.Request, tableName string, artifactName string) {
	documentArtifactRouteNotImplemented(w)
}

func (t *TableApi) ListDocumentArtifactManifests(w http.ResponseWriter, r *http.Request, tableName string, key string, params ListDocumentArtifactManifestsParams) {
	documentArtifactRouteNotImplemented(w)
}

func (t *TableApi) GetDocumentArtifactManifest(w http.ResponseWriter, r *http.Request, tableName string, key string, artifactName string, params GetDocumentArtifactManifestParams) {
	documentArtifactRouteNotImplemented(w)
}

func (t *TableApi) ReprocessDocumentArtifact(w http.ResponseWriter, r *http.Request, tableName string, key string, artifactName string) {
	documentArtifactRouteNotImplemented(w)
}
