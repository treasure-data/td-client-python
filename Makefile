clean:
	find . -name *.pyc -print0|xargs -0 rm
	find . -name '__pycache__' -print0|xargs -0 rm -rf
	rm -rf .tox/*
	rm -rf build/*
	rm -rf dist/*

