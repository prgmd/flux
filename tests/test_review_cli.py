import pytest

from flux.review.cli import main


def test_cli_never_defaults_to_human_and_rejects_unknown_evidence(tmp_path):
    args=['--journal',str(tmp_path),'--evidence','missing','--status','hold',
          '--reason','needs_check','--note','추가 근거 확인','--reviewer-id','test-reviewer']
    with pytest.raises(SystemExit):main(args)
    with pytest.raises(ValueError,match='unknown evidence'):main(args+['--reviewer-kind','agent'])
