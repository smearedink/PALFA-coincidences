PALFA-coincidences
==================
This repository contains the code used for matching candidates within the PALFA survey output as well as the code for the web interface.

A couple of notes:

* webviewer/match_data.db is the output of the matching code.

* The php coding in the web interface is pretty awful, in that I threw some code together and once it did what I wanted it to do I stopped. Of all the things I learned to do to make this interface (having done essentially no web coding before), php was the one I put the least effort into, and thus extending the php aspects of this code can be a bit painful at present. Since php seems pretty darned useful, I may learn to use it properly in the future and improve this.

* The matching code requires a big .npy file pulled from the PALFA database. I'm not putting that here, but the file has the following data columns (with one row for each candidate): "cand_id", "header_id", "obs_id", "beam_id", "proc_date", "topo_period", "bary_period", "dm", "presto_sigma", "prepfold_sigma", "ra_deg", "dec_deg", "mjd", "obs_time"
