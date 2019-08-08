clean: clean-sled clean-kv

clean-sled:
	-rm ./engine ./conf ./db ./snap.* 2>/dev/null || true

clean-kv:
	-rm ./engine ./*.meta ./*.log 2>/dev/null || true
