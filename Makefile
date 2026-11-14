.PHONY: up down restart smoke ingest bootstrap features ml predictions stream benchmark bi status all demo notebooks

up:        ; python scripts/stack.py up
down:      ; python scripts/stack.py down
restart:   ; python scripts/stack.py restart
smoke:     ; python scripts/stack.py smoke
ingest:    ; python scripts/stack.py ingest
bootstrap: ; python scripts/stack.py bootstrap
features:  ; python scripts/stack.py features
ml:        ; python scripts/stack.py ml
predictions: ; python scripts/stack.py load-predictions
stream:    ; python scripts/stack.py stream
benchmark: ; python scripts/stack.py benchmark
bi:        ; python scripts/stack.py bi
status:    ; python scripts/stack.py status
all:       ; python scripts/stack.py all
demo:      ; python scripts/demo.py
notebooks: ; python run_fullstack.py
