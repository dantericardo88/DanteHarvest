"""Unit tests for Phase 4 distillation pipeline."""

import pytest
from harvest_distill.segmentation.task_segmenter import TaskSegmenter
from harvest_distill.procedures.procedure_inferrer import ProcedureInferrer
from harvest_distill.packs.pack_builder import PackBuilder
from harvest_core.control.exceptions import PackagingError


def _make_action(action_type: str, timestamp: float, target: str = "#el") -> dict:
    return {"action_type": action_type, "timestamp": timestamp, "target_selector": target}


class TestTaskSegmenter:
    def setup_method(self):
        self.segmenter = TaskSegmenter(idle_gap_seconds=10.0, min_actions_per_span=1)

    def test_empty_actions_returns_empty(self):
        result = self.segmenter.segment([], "sess-1")
        assert result.span_count == 0
        assert result.total_actions == 0

    def test_single_action_produces_one_span(self):
        actions = [_make_action("click", 1.0)]
        result = self.segmenter.segment(actions, "sess-1")
        assert result.span_count == 1
        assert result.spans[0].action_count == 1

    def test_navigation_splits_span(self):
        actions = [
            _make_action("click", 1.0),
            _make_action("navigate", 2.0),
            _make_action("click", 3.0),
        ]
        result = self.segmenter.segment(actions, "sess-1")
        assert result.span_count >= 2

    def test_idle_gap_splits_span(self):
        actions = [
            _make_action("click", 1.0),
            _make_action("click", 100.0),  # 99s gap > 10s threshold
        ]
        result = self.segmenter.segment(actions, "sess-1")
        assert result.span_count == 2

    def test_span_ids_are_unique(self):
        actions = [
            _make_action("click", 1.0),
            _make_action("navigate", 2.0),
            _make_action("type", 3.0),
        ]
        result = self.segmenter.segment(actions, "sess-1")
        ids = [s.span_id for s in result.spans]
        assert len(ids) == len(set(ids))

    def test_to_dict_structure(self):
        actions = [_make_action("click", 1.0)]
        result = self.segmenter.segment(actions, "sess-1")
        d = result.to_dict()
        assert "spans" in d
        assert "total_actions" in d


class TestProcedureInferrer:
    def setup_method(self):
        self.inferrer = ProcedureInferrer(min_frequency=2, ngram_size=2)

    def test_empty_spans_returns_empty(self):
        result = self.inferrer.infer([])
        assert result.total_spans == 0
        assert result.graphs == []

    def test_repeated_pattern_produces_graph(self):
        spans = [
            {"span_id": "s1", "title": "t1", "actions": [
                {"action_type": "click"}, {"action_type": "type"}
            ]},
            {"span_id": "s2", "title": "t2", "actions": [
                {"action_type": "click"}, {"action_type": "type"}
            ]},
        ]
        result = self.inferrer.infer(spans)
        assert len(result.graphs) >= 1
        assert result.graphs[0].confidence > 0

    def test_unique_patterns_no_repeated_ngrams(self):
        spans = [
            {"span_id": "s1", "title": "t1", "actions": [{"action_type": "click"}]},
        ]
        result = self.inferrer.infer(spans)
        # Single span, no repeated n-grams → fallback graphs
        assert result.total_spans == 1

    def test_best_returns_highest_confidence(self):
        spans = [
            {"span_id": f"s{i}", "title": "t", "actions": [
                {"action_type": "click"}, {"action_type": "submit"}
            ]}
            for i in range(3)
        ]
        result = self.inferrer.infer(spans)
        best = result.best()
        if best and result.graphs:
            assert best.confidence == max(g.confidence for g in result.graphs)

    def test_graphs_sorted_by_confidence_desc(self):
        spans = [
            {"span_id": f"s{i}", "title": "t", "actions": [
                {"action_type": "click"}, {"action_type": "type"}
            ]}
            for i in range(4)
        ]
        result = self.inferrer.infer(spans)
        confidences = [g.confidence for g in result.graphs]
        assert confidences == sorted(confidences, reverse=True)


class TestPackBuilder:
    @pytest.mark.asyncio
    async def test_build_workflow_pack(self):
        from harvest_distill.procedures.procedure_inferrer import (
            ProcedureGraph, ProcedureStep,
        )
        import uuid
        steps = [
            ProcedureStep(step_id=str(uuid.uuid4()), action_type="click",
                          target_pattern=None, value_pattern=None, step_index=0),
            ProcedureStep(step_id=str(uuid.uuid4()), action_type="submit",
                          target_pattern=None, value_pattern=None, step_index=1),
        ]
        graph = ProcedureGraph(
            graph_id=str(uuid.uuid4()),
            title="click → submit",
            steps=steps,
            frequency=3,
            total_spans=5,
            confidence=0.6,
        )
        builder = PackBuilder()
        pack = await builder.build_workflow_pack(graph, run_id="r1", project_id="p1")
        assert pack.pack_id
        assert len(pack.steps) == 2
        assert pack.promotion_status.value == "candidate"

    @pytest.mark.asyncio
    async def test_build_skill_pack(self):
        from harvest_distill.procedures.procedure_inferrer import (
            ProcedureGraph, ProcedureStep,
        )
        import uuid
        steps = [ProcedureStep(
            step_id=str(uuid.uuid4()), action_type="type",
            target_pattern="#q", value_pattern=None, step_index=0,
        )]
        graph = ProcedureGraph(
            graph_id=str(uuid.uuid4()),
            title="type query",
            steps=steps,
            frequency=2, total_spans=3, confidence=0.67,
        )
        builder = PackBuilder()
        pack = await builder.build_skill_pack(graph, run_id="r1", project_id="p1",
                                               skill_name="type_query")
        assert pack.skill_name == "type_query"

    @pytest.mark.asyncio
    async def test_empty_graph_raises_packaging_error(self):
        from harvest_distill.procedures.procedure_inferrer import ProcedureGraph
        import uuid
        graph = ProcedureGraph(
            graph_id=str(uuid.uuid4()), title="empty",
            steps=[], frequency=1, total_spans=1, confidence=1.0,
        )
        builder = PackBuilder()
        with pytest.raises(PackagingError):
            await builder.build_workflow_pack(graph, run_id="r1", project_id="p1")

    @pytest.mark.asyncio
    async def test_pack_builder_emits_chain_signal(self, tmp_path):
        from harvest_distill.procedures.procedure_inferrer import (
            ProcedureGraph, ProcedureStep,
        )
        from harvest_core.provenance.chain_writer import ChainWriter
        import uuid
        writer = ChainWriter(tmp_path / "chain.jsonl", "r1")
        steps = [ProcedureStep(step_id=str(uuid.uuid4()), action_type="click",
                               target_pattern=None, value_pattern=None, step_index=0)]
        graph = ProcedureGraph(
            graph_id=str(uuid.uuid4()), title="click",
            steps=steps, frequency=2, total_spans=3, confidence=0.67,
        )
        builder = PackBuilder(writer)
        await builder.build_workflow_pack(graph, run_id="r1", project_id="p1")
        signals = [e.signal for e in writer.read_all()]
        assert "pack.created" in signals
