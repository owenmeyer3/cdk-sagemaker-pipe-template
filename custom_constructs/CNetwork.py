from aws_cdk import (aws_ec2 as ec2)
from constructs import Construct
import copy

class Network(Construct):
    def __init__(
            self,
            scope: Construct,
            construct_id:str,
            vpc_config:dict={},
            **kwargs
    ):
        #### Usage ####
        # https://docs.aws.amazon.com/cdk/api/v2/python/aws_cdk.aws_ec2/Vpc.html
        # Network(
        #     #Required
        #     scope, construct_id, vpc_config,
        #     # Common
        #     region=None,
        #     # Uncommon - other aws_ec2.Vpc constructor args
        # )
        ###############
        super().__init__(scope, construct_id)
        self.config = copy.deepcopy(vpc_config)

        # Import existing Vpc

        sg_ids = []
        self.security_groups = []
        for i, sg_id in enumerate(vpc_config["SECURITY_GROUPS"]):
            sg_ids.append(sg_id)
            self.security_groups.append(
                ec2.SecurityGroup.from_security_group_id(self, f"{construct_id}SG{i}", sg_id)
            )

        subnet_ids = []
        availability_zones = []
        route_table_ids = []
        self.private_subnets = []
        for i, s in enumerate(vpc_config["PRIVATE_SUBNETS"]):
            subnet_ids.append(s["ID"])
            availability_zones.append(s["AZ"])
            route_table_ids.append(s["ROUTE_TABLE"])
            self.private_subnets.append(ec2.Subnet.from_subnet_attributes(self, f"{construct_id}ExPrivateSubnet{i}", subnet_id=s["ID"], availability_zone=s["AZ"], route_table_id=s["ROUTE_TABLE"]))

        subnet_ids = []
        availability_zones = []
        route_table_ids = []
        self.public_subnets = []
        for i, s in enumerate(vpc_config["PUBLIC_SUBNETS"]):
            subnet_ids.append(s["ID"])
            availability_zones.append(s["AZ"])
            route_table_ids.append(s["ROUTE_TABLE"])
            self.public_subnets.append(ec2.Subnet.from_subnet_attributes(self, f"{construct_id}ExPublicSubnet{i}", subnet_id=s["ID"], availability_zone=s["AZ"], route_table_id=s["ROUTE_TABLE"]))
   
        self.vpc = ec2.Vpc.from_vpc_attributes(
            self,
            f"{construct_id}Vpc",
            availability_zones=availability_zones,
            vpc_id=vpc_config["ID"],
            private_subnet_ids=subnet_ids,
            private_subnet_route_table_ids=route_table_ids,
            **kwargs
        )

    def get_vpc(self):
        return self.vpc

    def get_security_groups(self):
        return self.security_groups

    def get_private_subnet_selection(self):
        return ec2.SubnetSelection(subnets=self.private_subnets)
    
    def get_private_subnets(self):
        return self.private_subnets

    def get_public_subnet_selection(self):
        return ec2.SubnetSelection(subnets=self.public_subnets)
    
    def get_public_subnets(self):
        return self.public_subnets

