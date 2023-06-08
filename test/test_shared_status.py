from crave_results import CraveResults
db = CraveResults()
db.shared_status_create('test_shared', [])
db.shared_status_get('test_shared')
def asd():
    st = db.shared_status_get('test_shared')
    st.append(55)
    db.shared_status_put('test_shared', st)

asd()

