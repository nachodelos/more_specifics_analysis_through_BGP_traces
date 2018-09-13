import pandas as pd
import experiment_manifest as exp
import file_manager as f


if __name__ == "__main__":
    print("---------------")
    print("Stage 7: Combine collector results")
    print("---------------")

    # VARIABLES (experiment)
    exp_name, coll= exp.load_arguments()
    experiments = getattr(exp, 'experiments')
    experiment = experiments[exp_name]

    from_date = experiment['initDay']
    to_date = experiment['endDay']
    result_directory = experiment['resultDirectory']
    file_ext = experiment['resultFormat']

    # Directories creation
    step_dir = '/7.combined_data'
    exp.per_step_dir(exp_name, step_dir)

    IP_versions = ['IPv4', 'IPv6']
    collectors = ['rrc00', 'rrc01', 'rrc04', 'rrc05', 'rrc07', 'rrc10', 'rrc11', 'rrc12', 'rrc13']

    frames = []

    for IP_v in IP_versions:
        output_file_path = result_directory + exp_name + '/7.combined_data/' + IP_v + '/' + from_date + '-' + to_date + '.csv'
        write_flag = f.overwrite_file(output_file_path)

        if write_flag:
            for coll in collectors:
                df = pd.read_csv(result_directory + exp_name + '/6.more_specifics_analysis/' + IP_v + '/' + coll + '_' + from_date + '-' + to_date + '.csv')
                df = df.drop(['Unnamed: 0'], axis=1)
                df['collector'] = coll
                df['IPv'] = IP_v
                frames.append(df)

    result = pd.concat(frames)
    result = result.reset_index(drop=True)
    output_file_path = result_directory + exp_name + step_dir + '/' + from_date + '-' + to_date + '.csv'
    f.save_file(result, file_ext, output_file_path)
