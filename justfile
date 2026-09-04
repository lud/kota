_mix_deps:
  out=$(mix deps.get) && echo "all dependencies fetched" || { echo "$out"; exit 1; }

test:
  mix test

_git_status:
  git status

format:
  mix format --migrate

credo:
  mix credo --strict --all

dialyzer:
  mix dialyzer

readmix:
  mix rdmx.update README.md

_libdev_check:
  mix libdev.check

check: _mix_deps format readmix _libdev_check _git_status
