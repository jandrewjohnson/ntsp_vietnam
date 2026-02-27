import os
import hazelbean as hb
import gtappy.gtappy_initialize_project as gtappy_initialize_project
import gtap_invest.gtap_invest_initialize_project as gtap_invest_initialize_project
import seals.seals_initialize_project as seals_initialize_project
import gtappy.gtappy_utils as gtappy_utils, gtappy.gtappy_tasks as gtappy_tasks


def build_ntsp_vietnam_task_tree(p):
    # import global_invest
    # from global_invest import ecosystem_services_tasks
    # p.project_aoi_task = p.add_task(ecosystem_services_tasks.project_aoi)     
    # p.lulc_clip_task = p.add_task(seals_generate_base_data.lulc_clip_quick)     
    p.base_data_as_csv_task = p.add_task(gtappy_tasks.base_data_as_csv) 
    p.initial_gtap_runs_task = p.add_task(gtappy_tasks.initial_gtap_runs)    
    # p.erosion_wide_to_tall_conversion_task = p.add_task(gtappy_tasks.erosion_wide_to_tall_conversion)              
    p.erosion_shock_har_task = p.add_task(gtappy_tasks.erosion_shock_har)              
    # p.erosion_shock_har_task = p.add_task(gtappy_tasks.pollination)              
    # p.erosion_shock_har_task = p.add_task(ecosystem_services_tasks.ntsp_pollination_economic_calculation)              
    p.erosion_shock_har_task = p.add_task(gtappy_tasks.pollination_shock_har)              
    
    p.gtap_runs_task = p.add_task(gtappy_tasks.gtap_runs)
    p.econ_results_task = p.add_task(gtappy_tasks.econ_results)
    p.raw_csvs_task = p.add_task(gtappy_tasks.raw_csvs, parent=p.econ_results_task)
    p.metadata_task = p.add_task(gtappy_tasks.metadata, parent=p.econ_results_task)
    p.variables_metadata_by_dims_task = p.add_task(gtappy_tasks.variables_metadata_by_dims, parent=p.econ_results_task)
    p.variables_by_dims_task = p.add_task(gtappy_tasks.variables_by_dims, parent=p.econ_results_task)
    p.vars_task = p.add_task(gtappy_tasks.vars, parent=p.econ_results_task)
    p.extended_vars_task = p.add_task(gtappy_tasks.extended_vars, parent=p.econ_results_task)
    p.tables_task = p.add_task(gtappy_tasks.tables, parent=p.econ_results_task)

    p.econ_visualization_task = p.add_task(gtappy_tasks.econ_visualization, skip_existing=0)
    p.custom_plots_task = p.add_task(gtappy_tasks.custom_plots, parent=p.econ_visualization_task, skip_existing=0)
         
  


if __name__ == '__main__':

    # Create a ProjectFlow Object to organize directories and enable parallel processing.
    p = hb.ProjectFlow()
    p.gtap_runs_dir = os.path.join(p.base_data_dir, 'gtappy/cge_releases/gtapv7-aez-rd/out')

    # Assign project-level attributes to the p object (such as in p.base_data_dir = ... below)
    # including where the project_dir and base_data are located.
    # The project_name is used to name the project directory below. If the directory exists, each task will not recreate
    # files that already exist. 
    p.user_dir = os.path.expanduser('~')        
    p.extra_dirs = ['Files', 'gtap_invest', 'projects', 'ntsp']
    p.project_name = 'ntsp_vietnam'
    # p.project_name = p.project43_name + '_' + hb.pretty_time() # If don't you want to recreate everything each time, comment out this line.
    
    # Based on the paths above, set the project_dir. All files will be created in this directory.
    p.project_dir = os.path.join(p.user_dir, os.sep.join(p.extra_dirs), p.project_name)
    p.set_project_dir(p.project_dir) 
    
    p.run_in_parallel = 1 # Must be set before building the task tree if the task tree has parralel iterator tasks.

    build_ntsp_vietnam_task_tree(p) 
    # gtappy_initialize_project.build_us_irrigation_task_tree(p) 
    
    # Set the base data dir. The model will check here to see if it has everything it needs to run.
    # If anything is missing, it will download it. You can use the same base_data dir across multiple projects.
    # Additionally, if you're clever, you can move files generated in your tasks to the right base_data_dir
    # directory so that they are available for future projects and avoids redundant processing.
    # The final directory has to be named base_data to match the naming convention on the google cloud bucket.
    p.base_data_dir = os.path.join(p.user_dir, 'Files/base_data')

    # ProjectFlow downloads all files automatically via the p.get_path() function. If you want it to download from a different 
    # bucket than default, provide the name and credentials here. Otherwise uses default public data 'gtap_invest_seals_2023_04_21'.
    p.data_credentials_path = None
    p.input_bucket_name = None
    
    # ODD NOTE: This must come before initialize_parameter_Definitions because that calls set_derived_attributes, which has a default option set.
    p.processing_resolution = 4.0 # In degrees. Must be in pyramid_compatible_resolutions
    
    
    # Parameters defined here are constant across scenarios
    p.parameter_definitions_filename = 'ntsp_vietnam_parameters.csv'
    p.parameter_definitions_path = os.path.join(p.input_dir, p.parameter_definitions_filename)
    gtappy_initialize_project.initialize_parameter_definitions(p)
       
    # Variables defined here are updated for each scenario row that is iterated over
    p.scenario_definitions_filename = 'ntsp_vietnam_scenarios.csv' 
    p.scenario_definitions_path = os.path.join(p.input_dir, p.scenario_definitions_filename)
    gtappy_initialize_project.initialize_scenario_definitions(p)
    
    # Variables defined here are updated for each scenario row that is iterated over
    p.output_definitions_filename = 'ntsp_vietnam_outputs.csv' 
    p.output_definitions_path = os.path.join(p.input_dir, p.output_definitions_filename)
    gtappy_initialize_project.initialize_output_definitions(p)
    
    ### HUGE ADDITION: Generates multiple coarse downscalings. this selects which to use for fine downscaling.
    p.aggregation_method_string = 'covariate_additive'
    # p.aggregation_method_string = 'covariate_multiply_regional_change_sum'
                      
    # # SEALS is based on an extremely comprehensive region classification system defined in the following geopackage.
    # global_regions_vector_ref_path = os.path.join('cartographic', 'ee', 'ee_r264_correspondence.gpkg')
    # p.global_regions_vector_path = p.get_path(global_regions_vector_ref_path)

    # # Set processing resolution: determines how large of a chunk should be processed at a time. 4 deg is about max for 64gb memory systems
    # p.processing_resolution = 1.0 # In degrees. Must be in pyramid_compatible_resolutions

    gtappy_initialize_project.set_advanced_options(p)
    
    
    seals_initialize_project.set_advanced_options(p)
    
    
    # This is a poorly supported option that chooses whether to use teh projections labor productivity veruss the endogenously determined one.
    # For the bau_no_es, we draw from the exogenous projections and swap those in for GDP. This makes countries have GDP growth rates that
    # match baseline projections. However, for subsequent scenarios, we want to use the endogenously determined labor productivity so that they are comparable
    p.force_standard_productivity = 0 # note this has to be after set_advanced_options cause it overrides that
    
    # In case you want to analyze the results of a statically provided model, can set this to True
    p.get_gtap_runs_dir_from_scenarios = 1 # If false, just assume that all of the files were put in a flat folder. this is how erwin currently does it.
    
    # Define where the GEMPACK solver/license is installed
    p.gempack_utils_dir = "C://GP" 


    
    
    # # Define wihch CGE release to use
    # # p.cge_model_release_string = 'gtapv7-aez-rd'    
    
    # p.template_bau_oldschool_cmf_dir = os.path.join(p.base_data_dir, 'gtappy', 'cge_releases', p.cge_model_release_string, 'cmf')
    # p.cmf_template_types = {i: 'bau_S2R45' for i in p.experiment_labels}
    # # p.cmf_template_types = {}
    # # p.cmf_template_types['bau'] = 'bau'
    # # p.cmf_template_types['bau_land_chl'] = 'bau'
    # # p.cmf_template_types['bau_tpreg'] = 'bau'
    # # p.cmf_template_types['bau_land_all'] = 'bau'
    # # p.template_bau_oldschool_cmf_path = os.path.join(p.base_data_dir, 'gtappy', 'cge_releases', p.cge_model_release_string, 'cmf', p.cge_model_release_string + '_bau.cmf') UNUSED
    # p.template_bau_es_cmf_path = os.path.join(p.base_data_dir, 'gtappy', 'cge_releases', p.cge_model_release_string, 'cmf', p.cge_model_release_string + '_bau_es.cmf')

    
    # NOTGE GTAPv7-aez is provided by default with each new model. It also tests if Hod-1 in prices.
    
    # Generate a nested dictionary for all permutations of aggregations and experiments. 
    # This will set xsets, xsubsets, and shocks attributes of the ProjectFlow object p.
    
    # TODOO REPLACE WITH SCENARIOS.csv functionality
    # gtappy_utils.set_attributes_based_on_aggregation_and_experiments(p, p.aggregation_labels, p.experiment_labels)
    
    
    # p.custom_gtap_executable_filename = None # TODO
    # p.cge_model_dir = os.path.join(p.base_data_dir, 'gtappy', 'cge_releases', p.cge_model_release_string)
    # p.cge_executable_path = os.path.join(p.cge_model_dir, 'mod', p.cge_model_release_string + '.exe')
    # p.cge_data_dir = os.path.join(p.base_data_dir, 'gtappy', 'cge_releases', p.cge_model_release_string, 'data') # Note I just changed this to NOT have the aggregation in it. will need to be fixed for regular gtappy    
    # p.cge_data_dir = os.path.join(p.cge_model_dir, 'data_aggregations') # Note I just changed this to NOT have the aggregation in it. will need to be fixed for regular gtappy    



    p.L = hb.get_logger('test_run_seals')
    hb.log('Created ProjectFlow object at ' + p.project_dir + '\n    from script ' + p.calling_script + '\n    with base_data set at ' + p.base_data_dir)
    
    p.execute()

    result = 'Done!'
    


    # Choose which econ model to use and then find its root path in the gtappy library
    # p.economic_model_release_string = 'gtap_v7_2022_08_04'
    # p.economic_model_root_dir = os.path.join(os.path.split(gtappy_cmf_generation.__file__)[0], 'cge_releases', p.economic_model_release_string)

    # TODOOO: bau_es not working becasue it first needs to copy the labor productivity shock from BAU to BAU_ES

    shock_descriptions = """
1) agricultural and food subsidies to encourage domestic consumption through eg tax or tariff on exports (aligns to SDG 12.2);

xset new_set (all the food stuff)
xsubset new_set is subset of COMM


shock txs(new_set, REG) = uniform 50



2) dietary change via tax on meat to reduce meat consumption

shock tpreg("ruminants AND better woyuld be processed foods meats", REG) = uniform 50

LUC ones

change c_MAXLAND(AEZS, REG) = uniform -0.3; # 30% of land is protected

3) Expansion of protected and conserved areas in a way that minimizes cost, ie take the cheapest 30% of each country out of production (aligns to KMGBF T3, SDG 15.1);
4) Expansion of protected and conserved areas to safeguard documented areas of particular importance for biodiversity, ie Key Biodiversity Areas (aligns to KMGBF T3, SDG 15.1);
5) Restoration of current production lands in a way that minimizes cost, ie transition the cheapest 30% of each country from production to natural land use (aligns to KMGBF T2)
    """
    
    


    
    ###------- Write the unique information that defines how each scenario's CMF is different.

    # Define AGGREGATION specific sets
    p.xsets['v11_s26_r50'] = []
    p.xsubsets['v11_s26_r50'] = []
    
 
    # Define SCENARIO specific information
    # p.shocks['v11_s26_r50']['bau_land_chl'] = [
    #     # 'shock aoall("cmt", REG) = uniform -20;',
    #     'change c_MAXLAND(AEZS, "chl") = uniform -0.75;',f
    # ]
    p.shocks['v11_s26_r50']['bau_tpreg'] = [
        # We need to know the power of the shock to know how to schok it.
        'percent_change c_MAXLAND(AEZS, "bra") = uniform -10;'


        # THESE ARE THE ONES E
        # 'shock tpd("cmt", REG) = uniform 50;' # For all domestic
        # 'shock tpm("cmt", REG) = uniform 50;' # For all imported
        # 'shock tpdall("cmt", REG) = uniform 50;' # For all domestic
        # 'shock tpmall("cmt", REG) = uniform 50;' # For all imported
        
        # 'shock txs("cmt", REG, REG) = uniform 50;' worked, also is a power
        # 'shock tpreg(REG) = uniform 50;'   # DIDNT SOLVE, is a uniform shifter cause affects all commodities.
    ]
    p.shocks['v11_s26_r50']['bau_land_all'] = [
        # 'change c_MAXLAND(AEZS, REG) = uniform -5;', # Change means interpreted as a raw ordinary change. so -5 means 5 less hectares.
        # 'change c_MAXLAND("AEZ2", "bra") = uniform -100000;', # Change means interpreted as a raw ordinary change. so -5 means 5 less hectares.
        # 'change c_MAXLAND("AEZ3", "bra") = uniform -100000;', # Change means interpreted as a raw ordinary change. so -5 means 5 less hectares.
        # 'change c_MAXLAND("AEZ4", "bra") = uniform -1000000;', # Change means interpreted as a raw ordinary change. so -5 means 5 less hectares.
        'change c_MAXLAND("AEZ5", "bra") = uniform -5;', # Change means interpreted as a raw ordinary change. so -5 means 5 less hectares.
        'change c_MAXLAND("AEZ6", "bra") = uniform -5;', # Change means interpreted as a raw ordinary change. so -5 means 5 less hectares.
        # 'change c_MAXLAND(AEZS, REG) = uniform -50;', doesn't solve
    ]
    # p.shocks['v11_s26_r50']['bau_txs'] = [
    #     'shock txs("cmt", REG, REG) = uniform 50;'
    # ]
    # p.shocks['v11_s26_r50']['bau_ssp3'] = [
    #     'swap qe(\"capital\",REG) = capadd(REG);'
    #     'swap afelabreg = qgdppcfisher;'
    #     'swap qesf = qesfsupply;'
    #     'shock del_unity = 1;',
    #     'shock qgdppcfisher = file <p1>/BaseScen.har header \"OGP2\" slice "<p5>";'
    #     'shock pop = file <p1>/BaseScen.har header "POP3" slice "<p5>";',
    #     'shock qe(ENDWL,REG) = file <p1>/BaseScen.har header \"LAB2\" slice \"<p5>\";'
    # ]
    # p.shocks['v11_s26_r50']['bau_ssp2_with_fire'] = [
    #     'swap qe(\"capital\",REG) = capadd(REG);',
    #     'swap afelabreg = qgdppcfisher;',
    #     'swap qesf = qesfsupply;',
    #     'shock del_unity = 1;',
    #     'shock qgdppcfisher = file <p1>/BaseScen.har header \"OGP2\" slice \"<p5>\";',
    #     'shock pop = file <p1>/BaseScen.har header "POP2" slice \"<p5>\";',
    #     'shock qe(ENDWL,REG) = file <p1>/NewHarFileWithPOP2AIR_Column.har header \"LAB2_fire\" slice \"<p5>\";',]
    
    
        #     ! (A1) Activate year-on-year capital accumulation equation
    # swap qe("capital",REG) = capadd(REG);

    # ! (A2) Endogenize labor productivity and exogenize GDP per capita
    # swap afelabreg = qgdppcfisher;

    # ! (A3) Upward sloping supply curve for sector-specific factor
    # swap qesf = qesfsupply;

    # !-----------------------------------
    # ! (B) Impose baseline shocks
    # !-----------------------------------
    # ! (B1) Activate year-on-year capital accumulation mechanism  
    # shock del_unity = 1;

    # ! (B2) Real GDP per capita projections
    # shock qgdppcfisher = file <p1>\BaseScen.har header "OGP2" slice "<p5>";

    # ! (B3) Population growth projections 
    # shock  pop = file <p1>\BaseScen.har header "POP2" slice "<p5>";   

    # ! (B4) Labor force growth projections
    # shock  qe(ENDWL,REG) = file <p1>\BaseScen.har header "LAB2" slice "<p5>";
    
    # Put any additional CMF commands here. These will overwrite things in the template cmf via a key = value string representation.
    # p.cmf_commands['v11-s26-r50']['bau'] = {'Steps': '6 12 18;', 'Method': 'euler;'}
    # p.cmf_commands['v11-s26-r50']['bau_es'] = {'Steps': '6 12 18;', 'Method': 'euler;'}
    

    

    # p.mapping_10_path = hb.get_first_extant_path(os.path.join('gtappy', 'aggregation_mappings', 'new_mapfile_data', 'mapping_10_regs_to_4_and_3_inc_categories.csv'), [p.input_dir, p.base_data_dir])
    # p.mapping_141_path = None # Not needed, cause no remapping is done here.


    
    