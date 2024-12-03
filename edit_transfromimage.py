###########################################################################################
#  This is a very basic script which you can correct the 'perspective distortion' on XenoCam
#  images. The principle is to calibrate the image captured by the camera to an object and
#  a surface of known size. In this case we use the alignment plate.
#  Every pixel on the output image is 1 cm. 
#  This is not a perfect way for doing the calculations. But to an extent this will give 
#  good estimations of the geometry
#  Note that the objects which are not on the road surface will appear distorted because 
#  the matrix which is in the below code is adapted for the road surface.
#  The error will be more if you measure objects which are farther. This is because the 
#  XenoCam needs to be focussed for the road surface close to the car.
#
###########################################################################################

import os
import cv2
import tqdm
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.widgets import Button
from concurrent.futures import ThreadPoolExecutor, as_completed



def transfromimage(input):
    
    # This matrix is fully adapted for your setup. If you change the orientation of the camera this value should change.
    matrix = np.asarray([[-4.72213304e-01,6.07375445e+00, -2.36383184e+01],
                        [ 9.15608405e-01,  2.65031461e+00, -7.51851357e+02],
                        [-2.53338772e-04,  4.22432334e-03,  1.00000000e+00]])
    
    angle = -90

    for root, dirs, files in os.walk(input):
        if 'PAVE' in root:
            print(root)
            images = os.listdir(root)
            for image_test in tqdm.tqdm(images):
                path = os.path.join(root, image_test)
                if path.endswith('.jpg'):
                    img_2 = cv2.imread(path)
                    img_2 = cv2.resize(img_2, (0,0), fx=0.5, fy=0.5)
                    corrected_img = cv2.warpPerspective(img_2, matrix, (1200, 1200))
                    
                    (h, w) = corrected_img.shape[:2]
                    center = (w // 2, h // 2)
                    rotation_matrix = cv2.getRotationMatrix2D(center, angle, 1.0)
                    rotated_img = cv2.warpAffine(corrected_img, rotation_matrix, (w, h))
                    
                    rotated_img = cv2.rotate(corrected_img, cv2.ROTATE_90_CLOCKWISE)
                    cnv_img_rgb = cv2.cvtColor(rotated_img, cv2.COLOR_BGR2RGB)
                                                
                    plt.imshow(cv2.cvtColor(cnv_img_rgb, cv2.COLOR_BGR2RGB))
                    plt.title('Corrected Image')
                    plt.axis('off')
                    plt.show()
                    
                    if not os.path.exists(root):
                        print('PAVE : Folder does not exist')
                    
                    cv2.imwrite(os.path.join(root,image_test),cv2.cvtColor(cnv_img_rgb, cv2.COLOR_BGR2RGB))

input = r'D:\xenomatix\output'
transfromimage(input)


# ----------------------


def transfromimage(folder_input):
    
    matrix = np.asarray([[-4.72213304e-01,6.07375445e+00, -2.36383184e+01],
                        [ 9.15608405e-01,  2.65031461e+00, -7.51851357e+02],
                        [-2.53338772e-04,  4.22432334e-03,  1.00000000e+00]])
    
    angle = -90
    countc = os.cpu_count()
    cpu = countc / 2
    
    with ThreadPoolExecutor(max_workers=cpu) as executor:
        futures = [executor.submit(process_single_image, root, image_test, matrix, angle) for root, dirs, files in os.walk(folder_input) 
                   if 'PAVE-0' in root for image_test in tqdm.tqdm(files) if image_test.endswith('.jpg')]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error processing image: {e}")

def process_single_image(root, image_test, matrix, angle):
    path = os.path.join(root, image_test)
    img_2 = cv2.imread(path)
    img_2 = cv2.resize(img_2, (0,0), fx=0.5, fy=0.5)
    corrected_img = cv2.warpPerspective(img_2, matrix, (1200, 1200))
                    
    (h, w) = corrected_img.shape[:2]
    center = (w // 2, h // 2)
    rotation_matrix = cv2.getRotationMatrix2D(center, angle, 1.0)
    rotated_img = cv2.warpAffine(corrected_img, rotation_matrix, (w, h))
                    
    rotated_img = cv2.rotate(corrected_img, cv2.ROTATE_90_CLOCKWISE)
    cnv_img_rgb = cv2.cvtColor(rotated_img, cv2.COLOR_BGR2RGB)
                   
    # plt.imshow(cv2.cvtColor(cnv_img_rgb, cv2.COLOR_BGR2RGB))
    # plt.title('Corrected Image')
    # plt.axis('off')
    # plt.show()
                    
    if not os.path.exists(root):
        print('PAVE : Folder does not exist')
                    
    cv2.imwrite(os.path.join(root,image_test),cv2.cvtColor(cnv_img_rgb, cv2.COLOR_BGR2RGB))

folder_input = r'D:\xenomatix\output'
transfromimage(folder_input)


# ----------------------


import concurrent.futures
import cv2
import os
import numpy as np
import matplotlib.pyplot as plt
import tqdm

def process_single_image(path, matrix, angle):
    img_2 = cv2.imread(path)
    img_2 = cv2.resize(img_2, (0, 0), fx=0.5, fy=0.5)
    corrected_img = cv2.warpPerspective(img_2, matrix, (1200, 1200))

    (h, w) = corrected_img.shape[:2]
    center = (w // 2, h // 2)
    rotation_matrix = cv2.getRotationMatrix2D(center, angle, 1.0)
    rotated_img = cv2.warpAffine(corrected_img, rotation_matrix, (w, h))
    rotated_img = cv2.rotate(corrected_img, cv2.ROTATE_90_CLOCKWISE)
    cnv_img_rgb = cv2.cvtColor(rotated_img, cv2.COLOR_BGR2RGB)
    
    cv2.imwrite(path, cv2.cvtColor(cnv_img_rgb, cv2.COLOR_BGR2RGB))

def transfromimage(input_folder):
    # Transformation matrix
    matrix = np.asarray([[-4.72213304e-01, 6.07375445e+00, -2.36383184e+01],
                         [9.15608405e-01, 2.65031461e+00, -7.51851357e+02],
                         [-2.53338772e-04, 4.22432334e-03, 1.00000000e+00]])

    angle = -90

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for root, dirs, files in os.walk(input_folder):
            if 'PAVE-0' in root:
                for image_test in files:
                    path = os.path.join(root, image_test)
                    if path.endswith('.jpg'):
                        futures.append(executor.submit(process_single_image, path, matrix, angle))
                        
        with tqdm.tqdm(total=len(futures)) as pbar:
            for future in concurrent.futures.as_completed(futures):
                future.result()
                pbar.update(1)

folder_input = r'D:\xenomatix\output'
transfromimage(folder_input)
                