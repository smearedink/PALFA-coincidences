# This is a little ad-hoc and has some things I'm not necessarily using anymore, but it gets the job done.

# This is also written to be run interactively--ie, in ipython, "run palfa_self_matching.py" and then after the initial steps are complete, "create_db()"

import numpy as np
import os, sys, cPickle, struct, base64, time
import sqlite3 as sql

files_basedir = "/data/lore/madsense/PALFA/coincidence_files"

all_mock_fname = "allcands_20141118_sortbyP.npy"
all_wapp_fname = "allcands_wapp_sortbyP.npy"

max_sep = 3.35 * 1.5 # arcmin

max_harm = 8

# this is multiplied by db_version and added to header_id or cand_id when necessary so that they are distinguishable
# eg, db v3 cand_id 11234567 -> 3011234567
dbvfact = 1000000000

mock_band = np.array([1214., 1537.])
#wapp_band = np.array([1390., 1490.])

# whether to check relative probability of false matches
#run_monte_carlo = False

# we'll just always go with the mock band, since it is more demanding in its matches
def ddm(dt, band=mock_band):
    return dt / (4.148808e3 * (band[0]**(-2) - band[1]**(-2)))

run_files_folder = "run_pickles"

cands2comp_fname = "cands2comp.pkl"
all_headers_fname = "all_headers.pkl"
neighbours_fname = "neighbours.pkl"
cands_by_header_fname = "cands_by_header.pkl"
groups_fname = "groups.pkl"
fixed_groups_fname = "fixed_groups.pkl"
fixed_groups_noshows_fname = "noshows.pkl"

if not os.path.exists(files_basedir+"/"+run_files_folder):
    os.makedirs(files_basedir+"/"+run_files_folder)

if os.path.exists(files_basedir+"/"+run_files_folder+"/"+cands2comp_fname) and os.path.exists(files_basedir+"/"+run_files_folder+"/"+all_headers_fname):
    with open(files_basedir+"/"+run_files_folder+"/"+cands2comp_fname, 'rb') as f:
        cands2comp = cPickle.load(f)
    with open(files_basedir+"/"+run_files_folder+"/"+all_headers_fname, 'rb') as f:
        all_headers = cPickle.load(f)
else:
    print "Generating cands2comp and headers files..."
    print "Loading all mock candidates..."
    all_palfa3 = np.load(files_basedir+"/"+all_mock_fname)
#    cands2comp_palfa3 = all_palfa3[["cand_id", "header_id", "obs_id", "beam_id", "proc_date", "topo_period", "bary_period", "dm", "presto_sigma", "ra_deg", "dec_deg", "mjd", "obs_time", "db_version"]][all_palfa3['dm'] > 10]
#    del all_palfa3
    print "Loading all wapp candidates..."
    all_wapp = np.load(files_basedir+"/"+all_wapp_fname)
#    cands2comp_wapp = all_wapp[all_wapp['dm'] > 10]
#    del all_wapp
    print "Combining..."
    all_palfa = np.concatenate((all_palfa3[["cand_id", "header_id", "obs_id", "beam_id", "proc_date", "topo_period", "bary_period", "dm", "presto_sigma", "ra_deg", "dec_deg", "mjd", "obs_time", "db_version"]], all_wapp))
    print "Freeing memory..."
    del all_palfa3, all_wapp
    print "Updating header_id and cand_id values..."
    all_palfa['header_id'] += dbvfact*all_palfa['db_version']
    all_palfa['cand_id'] += dbvfact*all_palfa['db_version']
    #sigma_cond = all_palfa3['prepfold_sigma'] > 7.
    #dm_cond = all_palfa3['dm'] > 10.
    #cands2comp = all_palfa3[sigma_cond * dm_cond]
    cands2comp = all_palfa[all_palfa['dm'] > 10]
#    cands2comp = np.concatenate((cands2comp_palfa3, cands2comp_wapp))
#    cands2comp['header_id'] += dbvfact*cands2comp['db_version']
#    cands2comp['cand_id'] += dbvfact*cands2comp['db_version']
#    del cands2comp_palfa3, cands2comp_wapp
    all_headers, all_headers_idx = np.unique(all_palfa['header_id'], return_index=True)
    all_headers = all_palfa[all_headers_idx][['header_id', 'obs_id', 'beam_id', 'ra_deg', 'dec_deg', 'mjd', 'obs_time', 'db_version']]
    with open(files_basedir+"/"+run_files_folder+"/"+cands2comp_fname, 'wb') as f:
        cPickle.dump(cands2comp, f, protocol=cPickle.HIGHEST_PROTOCOL)
    with open(files_basedir+"/"+run_files_folder+"/"+all_headers_fname, 'wb') as f:
        cPickle.dump(all_headers, f, protocol=cPickle.HIGHEST_PROTOCOL)
    print "Done."
nheaders = len(all_headers)
cands2comp_id2idx = dict(zip(cands2comp['cand_id'], range(len(cands2comp))))
all_headers_id2idx = dict(zip(all_headers['header_id'], range(len(all_headers))))


# BIG NOTE--there are cases where a long observation is split into two (maybe more sometimes) and for some
# reason these are treated separately in the pipeline.  they have the same mjd+beam_id, and the same filenames
# except for the last bit before .fits--one will be *.00000.fits and another *.00100.fits
# I don't understand WHY their mjd would be the same, or why they aren't combined into one file for processing
# Not quite sure what to do about them...


# using equirectangular approximation to get nearest neighbours
# note that this breaks down at poles and longitude (RA) wraps--the latter at least could be remedied fairly easily, but there is no need since there are no near neighbours across the vernal equinox in PALFA
def ang_sep_sq(ra1, dec1, ra2, dec2):
    """
    ra and dec values in radians
    returns angle in radians^2
    """
    x = (ra2 - ra1)*np.cos(0.5*(dec1 + dec2))
    y = dec2 - dec1
    return x**2 + y**2

# coordinates in radians
ra_rad = all_headers['ra_deg']*np.pi/180.
dec_rad = all_headers['dec_deg']*np.pi/180.
max_sep_rad_sq = (max_sep/60.*np.pi/180.)**2

if os.path.exists(files_basedir+"/"+run_files_folder+"/"+neighbours_fname):
    with open(files_basedir+"/"+run_files_folder+"/"+neighbours_fname, 'rb') as f:
        neighbours = cPickle.load(f)
else:
    print "No neighbours file found: generating %s" % neighbours_fname
    neighbours = {}
    for ii in range(len(all_headers)):
        neighbours_ii = list(all_headers['header_id'][ang_sep_sq(ra_rad[ii], dec_rad[ii], ra_rad, dec_rad) < max_sep_rad_sq])
        neighbours_ii.remove(all_headers['header_id'][ii])
        neighbours[all_headers['header_id'][ii]] = neighbours_ii
        sys.stdout.write("\rProgress: %-5.2f%%" % (100.*float(ii+1)/nheaders))
        sys.stdout.flush()
    sys.stdout.write("\n")
    sys.stdout.flush()
    with open(files_basedir+"/"+run_files_folder+"/"+neighbours_fname, 'wb') as f:
        cPickle.dump(neighbours, f, protocol=cPickle.HIGHEST_PROTOCOL)
    print "Done."

if os.path.exists(files_basedir+"/"+run_files_folder+"/"+cands_by_header_fname):
    with open(files_basedir+"/"+run_files_folder+"/"+cands_by_header_fname, 'rb') as f:
        cands_by_header = cPickle.load(f)
else:
    print "No cands_by_header file found: generating %s" % cands_by_header_fname
    cands_by_header = {}
    for ii, header_id in enumerate(all_headers['header_id']):
        cands_by_header[header_id] = cands2comp[cands2comp['header_id'] == header_id]
        sys.stdout.write("\rProgress: %-5.2f%%" % (100.*float(ii+1)/nheaders))
        sys.stdout.flush()
    sys.stdout.write("\n")
    sys.stdout.flush()
    with open(files_basedir+"/"+run_files_folder+"/"+cands_by_header_fname, 'wb') as f:
        cPickle.dump(cands_by_header, f, protocol=cPickle.HIGHEST_PROTOCOL)
    print "Done."

if os.path.exists(files_basedir+"/"+run_files_folder+"/"+groups_fname):
    with open(files_basedir+"/"+run_files_folder+"/"+groups_fname, 'rb') as f:
        groups = cPickle.load(f)
else:
    print "No groups file found: generating %s" % groups_fname
    t1 = time.time()
    checked = {}
    for k in all_headers['header_id']:
        checked[k] = set()
    groups = []
    for jj, this_id in enumerate(all_headers['header_id']):
        for that_id in neighbours[this_id]:
            if this_id not in checked[that_id]:
                these_cands = cands_by_header[this_id]
                those_cands = cands_by_header[that_id]
                for ii in xrange(len(these_cands)):
                    this_P = these_cands['bary_period'][ii]
                    this_DM = these_cands['dm'][ii]
                    #this_MJD = these_cands['mjd'][ii]
                    #simul = np.abs(this_MJD - those_cands['mjd']) < 1.e-5
                    dP_max = this_P**2 / cands2comp['obs_time'][ii]
                    # A conservative range that should allow for fast binary doppler variation
                    #dP_max = this_P * 0.001
                    
                    ### slower or faster than commented out version?  should check
                    # new one got to 1735 headers in 5.0 minutes
                    # old one got to 757 headers in 5.1 minutes (I wasn't paying attention)
                    # seems like the new one is better then
                    
                    match_idx = np.zeros(len(those_cands), dtype=bool)
                    for harm in [1./thing for thing in range(2, max_harm+1)][::-1] + range(1, max_harm+1):
                        P_match_idx = np.abs(those_cands['bary_period'] - this_P * harm) < dP_max * harm
                        DM_match_idx = np.abs(those_cands['dm'] - this_DM) < min(ddm(min(this_P*harm, this_P)), this_DM*0.1)
                        match_idx += (P_match_idx * DM_match_idx)
                    this_group = set(those_cands['cand_id'][match_idx]).union({these_cands['cand_id'][ii]})
                    if len(this_group) > 1:
                        group_match_ii = np.array([len(this_group.intersection(group)) for group in groups]).nonzero()[0]
                        if len(group_match_ii):
                            groups.append(set().union(*([groups[ii] for ii in group_match_ii] + [this_group])))
                            for ii in sorted(list(group_match_ii), reverse=True):
                                del groups[ii]
                        else:
                            groups.append(this_group)
                # prevent comparing twice
                checked[this_id].add(that_id)
        seconds_elapsed = time.time() - t1
        fraction_done = float(jj+1)/nheaders
        #sys.stdout.write("\rProgress: %-5.2f%%" % (100.*float(jj+1)/nheaders))
        sys.stdout.write("\rChecked %d of %d headers, %d groups, %.1f minutes passed" % (jj+1, nheaders, len(groups), seconds_elapsed/60.))
        sys.stdout.flush()
    sys.stdout.write("\n")
    sys.stdout.flush()
    with open(files_basedir+"/"+run_files_folder+"/"+groups_fname, 'wb') as f:
        cPickle.dump(groups, f, protocol=cPickle.HIGHEST_PROTOCOL)
    print "Done."

# now "fix" the groups so that if the same candidate from the same beam shows up twice
# (due to multi-fits-file observations) the older one is removed, and if this results in a
# group of one, the group is also removed

# example of this: headers 74921 and 75003

if os.path.exists(files_basedir+"/"+run_files_folder+"/"+fixed_groups_fname):
    with open(files_basedir+"/"+run_files_folder+"/"+fixed_groups_fname, 'rb') as f:
        fixed_groups = cPickle.load(f)
else:
    print "Removing multi-fits-file observation matches..."
    fixed_groups = []
    for ii in range(len(groups)):
        group_idx = [np.where(cands2comp['cand_id'] == cand_id)[0][0] for cand_id in groups[ii]]
#        group_cands_idx = np.array(group_idx)[np.argsort(cands2comp[group_idx], order='prepfold_sigma')[::-1]]
        group_cands_idx = np.array(group_idx)[np.argsort(cands2comp[group_idx], order='presto_sigma')[::-1]]
#        fixed_group_idx = group_cands_idx[np.unique(cands2comp[group_cands_idx][['obs_id', 'beam_id', 'db_version']], return_index=True)[1]]
        # a goofy workaround for the fact that np.unique's mergesort won't
        # work with an array of objects or tuples JUST in numpy 1.6.2
        # (the arbitrary numbers here are just biggish primes)
        fixed_group_idx = group_cands_idx[np.unique([(a[0]*271+a[1])*3319+a[2] for a in cands2comp[group_cands_idx][['obs_id', 'beam_id', 'db_version']]], return_index=True)[1]]
        fixed_group = set(cands2comp['cand_id'][fixed_group_idx])
        if len(fixed_group) > 1:
            fixed_groups.append(fixed_group)
    #groups = np.array(fixed_groups)
    #del fixed_groups
        sys.stdout.write("\rChecked %d of %d groups" % (ii+1, len(groups)))
        sys.stdout.flush()
    sys.stdout.write("\n")
    sys.stdout.flush()
    with open(files_basedir+"/"+run_files_folder+"/"+fixed_groups_fname, 'wb') as f:
        cPickle.dump(fixed_groups, f, protocol=cPickle.HIGHEST_PROTOCOL)
    print "Done."

if os.path.exists(files_basedir+"/"+run_files_folder+"/"+fixed_groups_noshows_fname):
    with open(files_basedir+"/"+run_files_folder+"/"+fixed_groups_noshows_fname, 'rb') as f:
        noshows = cPickle.load(f)
else:
    print "Finding no-shows..."
    noshows = []
    for group in fixed_groups:
        group_header_ids = [cands2comp[cands2comp_id2idx[cand]]['header_id'] for cand in group]
        group_headers_checked = set().union(*[neighbours[h] for h in group_header_ids])
        group_noshows = group_headers_checked.difference(group_header_ids)
        #group_noshows_info = all_headers[[np.where(all_headers['header_id'] == h)[0][0] for h in group_noshows]]
        noshows.append(group_noshows)
    with open(files_basedir+"/"+run_files_folder+"/"+fixed_groups_noshows_fname, 'wb') as f:
        cPickle.dump(noshows, f, protocol=cPickle.HIGHEST_PROTOCOL)
    print "Done."

### NOW THE DATABASE FUNCTIONS ###

def generate_group_id(cand_ids):
    """
    cand_ids is a list or array of the cand_id values in the group
    should return a unique string for that set of ids
    """
    return base64.b64encode(struct.pack('%dI'%len(cand_ids),\
        *np.sort(cand_ids)))

def create_db(out_fname="match_data.db", existing_db=None, remove_users=[]):
    """
    out_fname: output filename
    existing_db: if a previous db is entered here, the new db will populate
        the appropriate rows and columns with its data
    remove_users: if existing_db is used, list of usernames entered here will
        not be included in the new one
    """
    dbe_users_list = []
    dbe_groups_dict = {}
    if existing_db is not None:
        dbe = sql.connect(existing_db)
        dbe.row_factory = sql.Row
        dbe_cursor = dbe.cursor()
        dbe_users = dbe_cursor.execute("select * from users").fetchall()
        dbe_users_list = [g['username'] for g in dbe_users]
        for username in remove_users:
            if username in dbe_users_list:
                dbe_users_list.remove(username)
        dbe_groups = dbe_cursor.execute("select * from groups").fetchall()
        dbe_g_dicts = []
        for g in dbe_groups:
            g_dict = dict(g)
            for username in remove_users:
                g_dict.pop(username)
            dbe_g_dicts.append(g_dict)
        dbe_groups_dict = dict(zip([g['group_id'] for g in dbe_g_dicts],\
            dbe_g_dicts))
        dbe.close()
        # Can pop items from dictionary to get values and remove them from
        # dictionary. Whatever is left in the dictionary at the end should also
        # be included in the new groups list, since in the future run that
        # group may return.
    
    if os.path.exists(out_fname):
        os.remove(out_fname)
    
    atnf = np.loadtxt("atnf.txt", delimiter="\n", dtype=str)
    atnf_entries = [(p.split()[1], float(p.split()[2]), float(p.split()[3]),\
        float(p.split()[4]), float(p.split()[6])) for p in atnf]
    
    data = []
    for ii, group in enumerate(fixed_groups):
        cands = cands2comp[[cands2comp_id2idx[cand] for cand in group]]
        data.append(cands)

    cands_entries = []
    groups_entries = []
    noshows_entries = []
    new_group_ids = []
    for ii in range(len(data)):
        group_id = generate_group_id([item['cand_id'] for item in data[ii]])
        new_group_ids.append(group_id)
        periods = [item['bary_period'] for item in data[ii]]
        sigmas = [item['presto_sigma'] for item in data[ii]]
        ncands = len(periods)
        for cand in data[ii]:
            cands_entries.append((int(cand['cand_id'] % dbvfact), int(cand['cand_id'] / dbvfact), group_id, int(cand['header_id'] % dbvfact),\
                cand['bary_period'], cand['dm'], cand['presto_sigma']))
        for ns in noshows[ii]:
            noshows_entries.append((group_id, int(ns % dbvfact), int(ns / dbvfact)))
        groups_entries.append((group_id, float(np.min(periods)), float(np.max(periods)),\
            float(np.min(sigmas)), float(np.max(sigmas)), ncands))

    db = sql.connect(out_fname)
    cursor = db.cursor()
    
    cursor.execute("""
        CREATE TABLE cands(cand_id INTEGER, db_version INTEGER, group_id TEXT,
            header_id INTEGER, bary_period REAL, dm REAL, sigma REAL)
    """)
    
    cursor.execute("""
        CREATE TABLE noshows(group_id TEXT, header_id INTEGER, db_version INTEGER)
    """)
    
    cursor.execute("""
        CREATE TABLE groups(group_id TEXT PRIMARY KEY, min_period REAL,
            max_period REAL, min_sigma REAL, max_sigma REAL, ncands INTEGER)
    """)
    
    cursor.execute("""
        CREATE TABLE atnf(name TEXT, ra_deg REAL, dec_deg REAL, period REAL,
            dm REAL)
    """)
    
    cursor.execute("""
        CREATE TABLE headers(header_id INTEGER, db_version INTEGER, obs_id INTEGER,
            beam_id INTEGER, mjd REAL, ra_deg REAL, dec_deg REAL)
    """)
    
    cursor.execute("""
        CREATE TABLE users(username TEXT PRIMARY KEY)
    """)
    
    cursor.executemany("""
        INSERT INTO cands(cand_id, db_version, group_id, header_id, bary_period, dm, sigma)
        VALUES(?, ?, ?, ?, ?, ?, ?)
    """, cands_entries)
    
    cursor.executemany("""
        INSERT INTO noshows(group_id, header_id, db_version)
        VALUES(?, ?, ?)
    """, noshows_entries)
    
    cursor.executemany("""
        INSERT INTO groups(group_id, min_period, max_period, min_sigma,
            max_sigma, ncands)
        VALUES(?, ?, ?, ?, ?, ?)
    """, groups_entries)
    for username in dbe_users_list:
        cursor.execute("""
            ALTER TABLE groups ADD COLUMN "%s" INTEGER DEFAULT 0
        """ % username)
        cursor.execute("""
            INSERT INTO users(username) VALUES(?)
        """, (username,))
    
    cursor.executemany("""
        INSERT INTO atnf(name, ra_deg, dec_deg, period, dm)
        VALUES(?, ?, ?, ?, ?)
    """, atnf_entries)
    
    cursor.executemany("""
        INSERT INTO headers(header_id, db_version, obs_id, beam_id, mjd, ra_deg, dec_deg)
        VALUES(?, ?, ?, ?, ?, ?, ?)
    """, [(int(h['header_id']%dbvfact), int(h['db_version']), int(h['obs_id']), int(h['beam_id']), float(h['mjd']), float(h['ra_deg']), float(h['dec_deg'])) for h in all_headers])
    
    for new_group_id in new_group_ids:
        if new_group_id in dbe_groups_dict:
            group_info = dbe_groups_dict.pop(new_group_id)
            for username in dbe_users_list:
                cursor.execute("""
                    UPDATE groups SET "%s"=%d WHERE group_id="%s"
                """ % (username, group_info[username], new_group_id))

    if len(dbe_groups_dict):                
        cursor.execute("select * from groups").description
        col_names = [item[0] for item in cursor.description]
        col_names_str = str(tuple(col_names)) #.replace("'", "")
        new_entries = []
        for key in dbe_groups_dict:
            group_info = dbe_groups_dict[key]
            group_info['ncands'] = 0 # so we know this is from before
            new_entry = []
            for col_name in col_names:
                new_entry.append(group_info[col_name])
            new_entries.append(tuple(new_entry))
        cursor.executemany("""
            INSERT INTO groups%s
            VALUES%s
        """ % (col_names_str, str(tuple(['?']*len(col_names))).replace("'","")), new_entries)
        
    db.commit()
    db.close()

