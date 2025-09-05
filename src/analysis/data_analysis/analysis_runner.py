class AnalysisRunner:
    def __init__(self, cfg, upload_logger, pca_runner):
        self.cfg = cfg
        self.logger = upload_logger
        self.pca_runner = pca_runner
        self.steps = ["pca", "distribution", "rotated_pcs", "rotated_distribution", "t_test"]

    def run(self):
        key = {
            "exp_id": self.cfg.exp_id,
            "measurement_tp": self.cfg.measurement_tp,
            "device": self.cfg.device,
            "pain_groups": self.cfg.pain_groups,
            "nr_components": self.cfg.nr_components,
            "scaler_type": self.cfg.scaler_type, 
            "rotation_method": self.cfg.rotation_method,
        }
        entry = self.logger.get_entry(key)

# TODO: for the analysis, change all functions so they only get the cfg object as input
        # Step 1: PCA
        if not self.logger.is_uploaded(entry, "pca"):
            pca_results = self.pca_runner.run_pca_analysis(
                self.cfg.exp_id, self.cfg.measurement_tp, self.cfg.device,
                self.cfg.pain_groups, check_requirements=self.cfg.check_requirements
            )
            self._upload_step(entry, "pca", self.pca_runner.upload_pca_analysis, pca_results)

        # Step 2: Distribution
        dist_results = self.pca_runner.check_distribution(
            self.cfg.exp_id, self.cfg.device, self.cfg.measurement_tp,
            self.cfg.pain_groups, distributions_plotted=self.cfg.distributions_plotted
        )
        self._upload_step(entry, "distribution", self.pca_runner.upload_distribution, dist_results)

        # Step 3: Rotation
        if not self.logger.is_uploaded(entry, "rotated_pcs"):
            rot_results = self.pca_runner.run_pca_rotation(
                self.cfg.exp_id, self.cfg.device, self.cfg.measurement_tp,
                self.cfg.pain_groups, method=self.cfg.rotation_method
            )
            self._upload_step(entry, "rotated_pcs", self.pca_runner.upload_pca_analysis, rot_results)

        # Step 4: Rotated distribution
        rot_dist_results = self.pca_runner.check_distribution(
            self.cfg.exp_id, self.cfg.device, self.cfg.measurement_tp,
            self.cfg.pain_groups, distributions_plotted=self.cfg.distributions_plotted
        )
        self._upload_step(entry, "rotated_distribution", self.pca_runner.upload_distribution, rot_dist_results)

        # Step 5: t-test
        t_test_results = self.pca_runner.conduct_t_test(
            self.cfg.exp_id, self.cfg.device, self.cfg.measurement_tp, pain_groups=self.cfg.pain_groups
        )
        self._upload_step(entry, "t_test", self.pca_runner.upload_t_test, t_test_results)

        # Step 6: Reconstruction (always rerun)
        self.pca_runner.reconstruct_pcas(
            self.cfg.exp_id, self.cfg.device, self.cfg.measurement_tp,
            self.cfg.pain_groups, self.cfg.scaler_type, self.cfg.nr_components
        )
        self.pca_runner.reconstruct_pcas(
            self.cfg.exp_id, self.cfg.device, self.cfg.measurement_tp,
            self.cfg.pain_groups, self.cfg.scaler_type, self.cfg.nr_components,
            select_rotated=True
        )

    def _upload_step(self, entry, step, upload_func, result):
        if not self.logger.is_uploaded(entry, step):
            upload_func(result)
            self.logger.mark_uploaded(entry, step)
