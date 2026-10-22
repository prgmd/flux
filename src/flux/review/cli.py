import argparse
import json

from flux.review.journal import ReviewJournal


def main(argv=None):
    parser=argparse.ArgumentParser()
    parser.add_argument('--journal',required=True)
    parser.add_argument('--evidence',required=True)
    parser.add_argument('--status',required=True,choices=('approved','keep_separate','hold'))
    parser.add_argument('--reason',required=True)
    parser.add_argument('--note',required=True)
    parser.add_argument('--reviewer-kind',required=True,choices=('human','agent'))
    parser.add_argument('--reviewer-id',required=True)
    parser.add_argument('--supersedes')
    args=parser.parse_args(argv)
    result=ReviewJournal(args.journal).decide(args.evidence,status=args.status,reason_code=args.reason,
        note=args.note,reviewer_kind=args.reviewer_kind,reviewer_id=args.reviewer_id,supersedes=args.supersedes)
    print(json.dumps(result,ensure_ascii=False))
    return result


if __name__=='__main__':
    main()
