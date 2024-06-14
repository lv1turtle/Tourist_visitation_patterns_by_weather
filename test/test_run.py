import pytest
import sys
import os

def test_run():
    ret = pytest.main(["-v", "-k", "test_", "test"])
    
    sys.exit(ret)

# 코드가 직접 실행될때만 실행
if __name__=="__main__":
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    
    test_dir = os.path.join(cur_dir, "../../test")
    dags_dir = os.path.join(cur_dir, "../../dags")
    
    print(f"Current DIR : {sys.path}")
    
    sys.path.append(test_dir)
    sys.path.append(dags_dir)
    
    print(f"Fix current DIR : {sys.path}")
    
    test_run()    
