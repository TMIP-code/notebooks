# for OM2run in 1deg_jra55_iaf_omip2_cycle{3..3}; do
#     # change placeholders in script, submit, and clean up
#     sed "s/OM2run_placeholder/$OM2run/g" scripts/mld_ACCESS-OM2-1.sh > tmp.sh
#     qsub tmp.sh
#     rm tmp.sh
# done

# for OM2run in 025deg_jra55_iaf_omip2_cycle{1..5}; do
#     # change placeholders in script, submit, and clean up
#     sed "s/OM2run_placeholder/$OM2run/g" scripts/mld_ACCESS-OM2-025.sh > tmp.sh
#     qsub tmp.sh
#     rm tmp.sh
# done


# OM2-01runs=(
#     01deg_jra55v140_iaf
#     01deg_jra55v140_iaf_cycle2
#     01deg_jra55v140_iaf_cycle3
#     01deg_jra55v140_iaf_cycle4
#     01deg_jra55v140_iaf_cycle4_jra55v150_extension
#     01deg_jra55v150_iaf_cycle1
# )

# for OM2run in "01deg_jra55v150_iaf_cycle{1..1}"; do
#     # change placeholders in script, submit, and clean up
#     sed "s/OM2run_placeholder/$OM2run/g" scripts/mld_ACCESS-OM2-01.sh > tmp.sh
#     qsub tmp.sh
#     rm tmp.sh
# done

sed "s/OM2run_placeholder/025deg_jra55_iaf_omip2_cycle6/g" scripts/mld_ACCESS-OM2-025.sh > tmp.sh
qsub tmp.sh
rm tmp.sh