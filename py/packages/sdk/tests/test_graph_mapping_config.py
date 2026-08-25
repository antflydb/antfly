from antfly.client_generated.models.created_graph_artifact_producer_config import (
    CreatedGraphArtifactProducerConfig,
)
from antfly.client_generated.models.created_graph_index import CreatedGraphIndex
from antfly.client_generated.models.execution_policy import ExecutionPolicy
from antfly.client_generated.models.graph_algebraic_planning_config import (
    GraphAlgebraicPlanningConfig,
)
from antfly.client_generated.models.graph_artifact_edge_mapping_config import (
    GraphArtifactEdgeMappingConfig,
)
from antfly.client_generated.models.graph_artifact_node_mapping_config import (
    GraphArtifactNodeMappingConfig,
)
from antfly.client_generated.models.graph_artifact_node_mapping_config_model import (
    GraphArtifactNodeMappingConfigModel,
)
from antfly.client_generated.models.graph_artifact_producer_source_config import (
    GraphArtifactProducerSourceConfig,
)
from antfly.client_generated.models.graph_artifact_producer_source_config_type import (
    GraphArtifactProducerSourceConfigType,
)
from antfly.client_generated.models.graph_source_artifact_status import GraphSourceArtifactStatus
from antfly.client_generated.types import UNSET


def test_created_graph_index_exposes_artifact_mapping_and_planning() -> None:
    created = CreatedGraphIndex.from_dict(
        {
            "name": "relations_graph",
            "type": "graph",
            "sources": [
                {
                    "artifact": "relations_v1",
                    "nodes": {
                        "model": "document",
                        "source": "{{ _doc.key }}",
                        "target": "{{ _item.target.text }}",
                    },
                    "edge": {
                        "type": "{{ _item.type }}",
                        "weight": 0.75,
                        "metadata": {"source": "extractor"},
                    },
                    "context": {"doc_fields": ["title", "body"]},
                }
            ],
            "artifact": {
                "name": "relations_v1",
                "kind": "asset",
                "source": {"type": "template", "value": "{{ body }}"},
                "execution": {"batch_items": 8},
            },
            "algebraic_planning": {
                "bounded_traversal": {
                    "law": "provenance_semiring",
                }
            },
        }
    )

    assert isinstance(created.sources, list)
    assert len(created.sources) == 1
    source = created.sources[0]
    assert source.artifact == "relations_v1"
    assert isinstance(source.nodes, GraphArtifactNodeMappingConfig)
    assert source.nodes.model is GraphArtifactNodeMappingConfigModel.DOCUMENT
    assert isinstance(source.edge, GraphArtifactEdgeMappingConfig)
    assert source.edge.weight == 0.75
    assert isinstance(created.algebraic_planning, GraphAlgebraicPlanningConfig)
    assert isinstance(created.artifact, CreatedGraphArtifactProducerConfig)
    assert isinstance(created.artifact.source, GraphArtifactProducerSourceConfig)
    assert created.artifact.source.type_ is GraphArtifactProducerSourceConfigType.TEMPLATE
    assert created.artifact.source.value == "{{ body }}"
    assert isinstance(created.artifact.execution, ExecutionPolicy)
    assert created.artifact.execution.batch_items == 8
    assert created.to_dict()["algebraic_planning"]["bounded_traversal"]["law"] == ("provenance_semiring")


def test_graph_source_status_reads_old_and_new_server_shapes() -> None:
    legacy = GraphSourceArtifactStatus.from_dict(
        {
            "name": "relations_v1",
            "path": "$.relations[*]",
            "format": "extraction_relation",
            "materialization_pending": True,
        }
    )
    assert legacy.artifact is UNSET

    current = GraphSourceArtifactStatus.from_dict(
        {
            "artifact": "relations_v1",
            "name": "relations_v1",
            "path": "$.relations[*]",
            "format": "extraction_relation",
            "materialization_pending": False,
        }
    )
    assert current.artifact == "relations_v1"
